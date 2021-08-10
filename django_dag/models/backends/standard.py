import sys
from collections import defaultdict

from django.db import models
from django_dag.exceptions import NodeNotReachableException
from django.db.models.query import QuerySet
from django.db.models.expressions import (
    Case,
    F,
    Value,
    When,
)
from django.db.models.functions import (
    Cast,
    Concat,
    LPad,
)
from .base import BaseNode


ProtoNodeManager = models.Manager
ProtoEdgeManager = models.Manager
ProtoEdgeQuerySet = QuerySet


_PATH_PADDING_SIZE = 4
_QUERY_ORDER_FIELDNAME = 'dag_order_sequence'

QUERY_ORDER_FIELDNAME_FORMAT = 'dag_%(name)s_sequence'
QUERY_PATH_FIELDNAME_FORMAT = 'dag_%(name)s_path'
QUERY_DEPTH_FIELDNAME = 'dag_depth'


def filter_order_with_annotations(queryset,
    field_names=[], field_values=[], annotations=[],
    empty_annotations=[],
    sequence_name=_QUERY_ORDER_FIELDNAME, offset=0):
    """
    Filter a queryset to match a predefined order pattern.

    The constructed query filters the items to those matching the field_values and
    applied any annotations to the items are required.
    If the filtered query contains duplicated then a union query is returned.

    if sequence_name is set or left default then this field can be used to order the query
    so that the result set matches that of the items in field_values.

    :param queryset `<QuerySet>`: The initial queryset to filter
    :param field_names Iterable<str>: List of filed names to used to identify an
        element of the resultant query ie ['pk',]
    :param field_values Iterable<Dict>: List of dicts where the keys/values match the names
        within the field_names paramater and the value is the condition value.
        Each element in the list is used to identify 1 item in the resultant query in order.
    :param annotations Iterable<Dict>: Annotations to be applied to each element.
        For each element in the list the key/value are used to annotate  1 item in the
        resultant query. The key value is the annotation name and the value is the value of
        the items annotation.
    :param sequence_name str: The name of the sequence field to be applied to the query. This
        then can be used to order the query if needed.
        If None then the items are not annotated with there sequence.
    :param offset int: The orders starting offset default: 0
    """

    order_case = []
    annotations_lists = defaultdict(list)
    used = []
    query = queryset.none()
    filter_condition = defaultdict(list)
    pos = None
    for pos, instance_value in enumerate(field_values):
        when_condition = {}
        for fn, fv in zip(field_names, instance_value):
            when_condition.update({fn: fv, })
        if when_condition in used:
            # Start new query and union
            query = filter_order_with_annotations(
                queryset,
                field_names=field_names,
                field_values=field_values[pos:],
                annotations=annotations[pos:],
                offset=pos,
                sequence_name=sequence_name
            )
            break
        for fn, fv in when_condition.items():
            filter_condition[f'{fn}__in'].append(fv)
        for ak, av in annotations[pos].items():
            anno_when = when_condition.copy()
            anno_when.update({'then': av})
            annotations_lists[ak].append(When(**anno_when))
        if sequence_name:
            anno_when = when_condition.copy()
            anno_when.update(
                {'then': Cast(Value(pos+offset), output_field=models.IntegerField())})
            annotations_lists[sequence_name].append(When(**anno_when))
        used.append(when_condition.copy())
        when_condition.update({'then': offset+pos})
        order_case.append(When(**when_condition))

    if pos is None:
        return query.annotate(**{name: Value(None, output_field=models.IntegerField()) for name in empty_annotations})

    annotations_cases = {ak: Case(*av) for ak, av in annotations_lists.items()}
    order_by = Case(*order_case)
    query = query.union(
        queryset.filter(**filter_condition)
        .annotate(**annotations_cases)
    )
    return query


class ProtoNodeQuerySet(QuerySet):
    padsize = _PATH_PADDING_SIZE

    def _LPad_sql(self, value, padsize):
        return LPad(
            Cast(value, output_field=models.TextField()),
            padsize, Value('0'))

    def _LPad_py(self, value, padsize):
        return str(value).zfill(padsize)

    def with_top_down(self, *args, padsize=_PATH_PADDING_SIZE, **kwargs):
        """
        Generates a query that does a top-to-bottom traversal without regard to any
        possible (left-to-right) ordering of the nodes

        :param padsize int: Length of the field segment for each node in pits path
            to it root.
        """
        return self._sort_query(*args,
            padsize=padsize, sort_name='top_down' ,**kwargs)

    def with_depth_first(self, *args, padsize=_PATH_PADDING_SIZE, **kwargs):
        """
        Generates a query that does a depth-first traversal, this account for the nodes
        sequence ordering (left-to-right) of the nodes.

        :param padsize int: Length of the field segment for each node in pits path
            to it root.
        """
        sequence_field = None
        if self.model.sequence_manager:
            sequence_field = self.model.sequence_manager \
                .get_node_rel_sort_query_component(
                    self.model, 'child', 'parent',
                    parent_filter_ref=models.OuterRef('path_parent_ref')
                )
        return self._sort_query(*args,
            padsize=padsize, sort_name='depth_first',
            sequence_field=sequence_field, **kwargs)

    def _sort_query(self, *args,
                    sort_name='sort',
                    sequence_field=None,
                    padsize=_PATH_PADDING_SIZE):

        _sequence_field = sequence_field if sequence_field else F('id')
        path_filedname = QUERY_PATH_FIELDNAME_FORMAT % {'name': sort_name, }
        node_model = self.model.get_node_model()
        pks = list(map(lambda x: x.pk, self))

        def child_values(roots):
            for f in roots:
                base_path = getattr(f, path_filedname)
                depth = getattr(f, QUERY_DEPTH_FIELDNAME) + 1
                if f.pk in pks:
                    yield f
                yield from child_values(
                    f.children.annotate(
                        **{
                            'path_parent_ref': Value(
                                int(f.pk), output_field=models.IntegerField()),
                            path_filedname: Concat(
                                Value(base_path+","),
                                self._LPad_sql(_sequence_field, padsize),
                            ),
                            QUERY_DEPTH_FIELDNAME: Cast(
                                Value(depth),
                                output_field=models.IntegerField()
                            ),
                        }
                    ))

        roots = node_model.objects.roots() \
            .annotate(**{
                path_filedname: self._LPad_sql(F('id'), padsize),
                QUERY_DEPTH_FIELDNAME: Cast(
                    Value(0),
                    output_field=models.IntegerField()
                )
            })
        data = list(child_values(roots))
        return node_model._convert_to_lazy_node_query(
            data,
            filter_order_with_annotations(
                node_model.objects,
                field_names=['id'],
                field_values=[(d.pk,) for d in data],
                annotations=[
                    {
                        path_filedname: Cast(
                            Value(getattr(d, path_filedname)),
                            output_field=models.TextField()
                        ),
                        QUERY_DEPTH_FIELDNAME: Cast(
                            Value(getattr(d, QUERY_DEPTH_FIELDNAME)),
                            output_field=models.IntegerField()
                        ),
                    }
                    for d in data
                ],
                empty_annotations=[path_filedname, QUERY_DEPTH_FIELDNAME],
                sequence_name=None
            )
        )


class ProtoNode(BaseNode):
    ################################################################
    # Public API
    @property
    def descendants(self):
        return self._convert_to_lazy_node_query(self._get_descendant())

    def get_descendant_pks(self):
        return list(self._get_descendant(node_to_cache_attr=lambda x: x.pk))

    def _get_descendant(self, cached_results=None, node_to_cache_attr=lambda x: x):
        if cached_results is None:
            cached_results = dict()
        if node_to_cache_attr(self) in cached_results.keys():
            return cached_results[node_to_cache_attr(self)]
        else:
            res = set()
            for f in self.children.all():
                res.add(node_to_cache_attr(f))
                res.update(f._get_descendant(
                    cached_results=cached_results,
                    node_to_cache_attr=node_to_cache_attr
                ))
            cached_results[node_to_cache_attr(self)] = res
            return res

    @property
    def ancestors(self):
        return self._convert_to_lazy_node_query(self._get_ancestor())

    def get_ancestor_pks(self):
        return list(self._get_ancestor(node_to_cache_attr=lambda x: x.pk))

    def _get_ancestor(self, cached_results=None, node_to_cache_attr=lambda x: x):
        if cached_results is None:
            cached_results = dict()
        if node_to_cache_attr(self) in cached_results.keys():
            return cached_results[node_to_cache_attr(self)]
        else:
            res = set()
            for f in self.parents.all():
                res.add(node_to_cache_attr(f))
                res.update(f._get_ancestor(
                    cached_results=cached_results,
                    node_to_cache_attr=node_to_cache_attr
                ))
            cached_results[node_to_cache_attr(self)] = res
            return res

    def get_paths(self, target, use_edges=False, downwards=None):
        try:
            if downwards is None or downwards is True:
                return self._get_paths(target, use_edges=use_edges, downwards=True)
        except NodeNotReachableException as err:
            if downwards is True:
                raise
        return target._get_paths(self, use_edges=use_edges, downwards=False)

    def _get_paths(self, target, use_edges=False, downwards=True):
        if self == target:
            # In principle can only be 1 zero length path, it also has no edge
            # so we can always return [] for the path. It can't have an edge, because
            # a self link forms a cycle of legnth one, and we try to guarantee being
            # cycle free (this as Directed-ACYCLIC-Graph)
            return [[], ]

        if target in self.children.all():
            # If the target is a child of the source object there can only
            # be 1 shortest path
            if use_edges:
                return [[e] for e in self.get_edge_model().objects.filter(
                    child=target,
                    parent=self
                )]
            else:
                return [[target if downwards else self], ]

        if target.pk in self.get_descendant_pks():
            paths = []
            path_length = sys.maxsize
            childItems = self.get_edge_model().objects.filter(
                parent=self
            )

            for child_edge in childItems:
                # Select the element in the return data struct.
                if use_edges:
                    element = child_edge
                else:
                    element = child_edge.child if downwards else child_edge.parent

                try:
                    # Use ourselves recursively to find the rest of
                    # the path (if extant) from each of our children
                    desc_paths = child_edge.child._get_paths(
                        target,
                        use_edges=use_edges,
                        downwards=downwards)
                    desc_path_length = len(desc_paths[0]) + 1

                    if desc_path_length < path_length:
                        # We found a short path than anything before, so replace.
                        paths = [[element] + subpath for subpath in desc_paths]
                        path_length = len(paths[0])
                    elif desc_path_length == path_length:
                        # We found a path of equal length so append to results
                        equal_paths = [[element] +
                                       subpath for subpath in desc_paths]
                        paths.extend(equal_paths)
                    # else a short path has already found so skip recording this one

                except NodeNotReachableException:
                    pass
        else:
            raise NodeNotReachableException()
        return paths

    def get_roots(self):
        at = self.get_ancestors_tree()
        roots = set()
        for a in at:
            roots.update(a._get_roots(at[a]))
        return self._convert_to_lazy_node_query(roots or set([self]))

    def _get_roots(self, at):
        """
        Works on objects: no queries
        """
        if not at:
            return set([self])
        roots = set()
        for a2 in at:
            roots.update(a2._get_roots(at[a2]))
        return roots

    def get_leaves(self):
        dt = self.get_descendants_tree()
        leaves = set()
        for d in dt:
            leaves.update(d._get_leaves(dt[d]))
        return self._convert_to_lazy_node_query(leaves or set([self]))

    def _get_leaves(self, dt):
        """
        Works on objects: no queries
        """
        if not dt:
            return set([self])
        leaves = set()
        for d2 in dt:
            leaves.update(d2._get_leaves(dt[d2]))
        return leaves

    def get_descendants_tree(self):
        """
        Returns a tree-like structure with progeny
        """
        tree = {}
        for f in self.children.all():
            tree[f] = f.get_descendants_tree()
        return tree

    def get_ancestors_tree(self):
        """
        Returns a tree-like structure with ancestors
        """
        tree = {}
        for f in self.parents.all():
            tree[f] = f.get_ancestors_tree()
        return tree

    @classmethod
    def _convert_to_lazy_node_query(cls, data, query=None):
        if query is None:
            fixedquery = cls.get_node_model().objects.filter(
                pk__in=map(lambda x: x.pk, data)
            )
        else:
            fixedquery = query._clone()
        fixedquery._result_cache = list(data)
        return fixedquery
