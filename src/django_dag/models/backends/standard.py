import sys
from collections import defaultdict
from itertools import chain
from functools import partial
from django.db import models, NotSupportedError
from django_dag.exceptions import NodeNotReachableException
from django.db.models.query import EmptyQuerySet, QuerySet
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
    RPad,
)
from django_delayed_union.base import DelayedQuerySetMethod
from .base import BaseNode
from .query import DagBaseDelayedUnionQuerySet
from . import (
    QUERY_PATH_FIELDNAME_FORMAT,
    QUERY_DEPTH_FIELDNAME,
    QUERY_NODE_PATH,
)


ProtoNodeManager = models.Manager
ProtoEdgeManager = models.Manager
ProtoEdgeQuerySet = QuerySet


_QUERY_ORDER_FIELDNAME = 'dag_order_sequence'


class ReflowPrimeQueryMethod(DelayedQuerySetMethod):
    """
    """
    def __call__(self, obj, *args, **kwargs):
        # assert len(obj._querysets) == 2, "Can only work on DAG querysets"
        prefetched = obj._result_cache
        assert kwargs.pop('prefetched', None) is None, "cannot use due prefetch sources"

        cloned_obj = obj._querysets[0]._clone()
        return getattr(cloned_obj, self.name)(
            *args, prefetched=prefetched, **kwargs)

    def get_base_docstring(self):
        return """
        Returns a new delayed queryset with `{name}(...)`` having been called
        on each of the component querysets.:
        """


class DagDelayedUnionQuerySet(DagBaseDelayedUnionQuerySet):
    """
    A delayed union query where the first query is considered
    the PRIME
    """
    with_sort_sequence = ReflowPrimeQueryMethod()
    distinct_node = ReflowPrimeQueryMethod()


def filter_order_with_annotations(queryset,
        field_names=[], values=[], annotations=[],
        empty_annotations=[],
        sequence_name=_QUERY_ORDER_FIELDNAME,
        offset=0):
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
    annotations_lists = defaultdict(list)
    used = []
    query = queryset.none()
    filter_condition = defaultdict(list)
    pos = None
    for pos, instance_value in enumerate(values):
        when_condition = {}
        for fn in field_names:
            fv = getattr(instance_value, fn)
            when_condition.update({fn: fv, })
        if when_condition in used:
            # Start new query and union
            query = filter_order_with_annotations(
                queryset,
                field_names=field_names,
                values=values[pos:],
                annotations=annotations[pos:],
                offset=pos,
                sequence_name=sequence_name,
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
                {'then': Cast(Value(pos + offset), output_field=models.IntegerField())})
            annotations_lists[sequence_name].append(When(**anno_when))
        used.append(when_condition.copy())

    if pos is None:
        return query.annotate(
            **{name: Value(None, output_field=models.IntegerField()) for name in empty_annotations}
        )

    annotations_cases = {ak: Case(*av) for ak, av in annotations_lists.items()}
    querypart = queryset.filter(**filter_condition) \
        .annotate(**annotations_cases)

    if isinstance(query, EmptyQuerySet):
        return querypart
    return DagDelayedUnionQuerySet(
        query, querypart
    )


class ProtoNodeQuerySet(QuerySet):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _LPad_sql(self, value, padding_size=None, padding_char=None):
        return LPad(
            Cast(value, output_field=models.TextField()),
            padding_size, Value(padding_char))

    def _LPad_py(self, value, padding_size=None, padding_char=None):
        return str(value).rjust(padding_size, padding_char)

    def _RPad_sql(self, value, padding_size=None, padding_char=None):
        return RPad(
            Cast(value, output_field=models.TextField()),
            padding_size, Value(padding_char))

    def _RPad_py(self, value, padding_size=None, padding_char=None):
        return str(value).ljust(padding_size, padding_char)

    def _padding_fn(self, padding_size, sql=True):
        if padding_size is None or padding_size == 0:
            # No Padding
            if sql:
                return lambda x, **k: Cast(x, output_field=models.CharField())
            return lambda x, **k: x
        elif padding_size > 0:
            return self._LPad_sql if sql else self._LPad_py
        return self._RPad_sql if sql else self._RPad_py

    def Pad(self, value, padding_size=None, padding_char=None, sql=True):
        padding_size = padding_size if padding_size is not None else self._padding_size
        padding_char = padding_char if padding_char is not None else self._padding_char
        return self._padding_fn(padding_size, sql=sql)(
            value, padding_size=abs(padding_size), padding_char=padding_char)

    def distinct_node(self, order_field: str, roots: QuerySet = None, **kwargs):
        """
        Modifies the query so that with nodes as distinct.

        As withing the DAG node can occur a number of times, this add support for a
        topological like view of the dag, where only the first visit to a node is included.
        This must be called after the initial query to add the order clause is run

        :param order_field (str): The name of the field or annotation the is used to
            order the 'distinct' nodes to determin the first
        """
        prefetched = kwargs.get('prefetched', None)
        if prefetched is None:
            raise NotSupportedError(
                'Cannot apply node distinction until a node sequence is applied')

        # Filter data for first pk match
        def data_filter(in_data):
            found = set()
            for item in in_data:
                if item.pk not in found:
                    found.add(item.pk)
                    yield item

        annotations_fields = [
            key
            for key in self.query.annotations.keys()
            if key not in [
                QUERY_DEPTH_FIELDNAME,
                QUERY_NODE_PATH
            ] and key.startswith('dag_')
        ]

        out_data = list(data_filter(prefetched))
        model = self.model.get_node_model()
        return model._convert_to_lazy_node_query(
            out_data,
            self._build_query_fn(out_data, model=model, annotations_fields=annotations_fields)
        )

    def with_pk_path(self, *args, name=None, **kwargs):
        """
        Generates a query that does a top-to-bottom traversal without regard to any
        possible (left-to-right) ordering of the nodes
        """
        if name is None:
            name = QUERY_PATH_FIELDNAME_FORMAT % {'name': 'pk', }

        model, data, query_fn = self._sort_query(*args,
            path_filedname=name, **kwargs)

        return model._convert_to_lazy_node_query(
            data,
            query_fn(data)
        )

    def with_sequence_path(self, *args, name=None, **kwargs):
        """
        Generates a query that does a depth-first traversal, this account for the nodes
        sequence ordering (left-to-right) of the nodes.
        """
        sequence_field = None
        if self.model.sequence_manager:
            sequence_field = self.model.sequence_manager \
                .get_node_rel_sort_query_component(
                    self.model, 'child', 'parent',
                    parent_filter_ref=models.OuterRef('path_parent_ref')
                )
        if name is None:
            name = QUERY_PATH_FIELDNAME_FORMAT % {'name': 'sequence', }

        model, data, query_fn = self._sort_query(
                *args,
                path_filedname=name,
                sequence_field=sequence_field,
                **kwargs)

        return model._convert_to_lazy_node_query(
            data,
            query_fn(data)
        )

    @staticmethod
    def _build_query_fn(querydata, *args, **kwargs):
        model = kwargs.get('model')
        annotations_fields = kwargs.get('annotations_fields')
        return filter_order_with_annotations(
            model.objects,
            field_names=['id'],
            values=querydata,
            annotations=[
                dict(
                    chain(
                        [
                            (
                                QUERY_DEPTH_FIELDNAME, Cast(
                                    Value(getattr(d, QUERY_DEPTH_FIELDNAME)),
                                    output_field=models.IntegerField())
                            ),
                            (
                                QUERY_NODE_PATH, Cast(
                                    Value(getattr(d, QUERY_NODE_PATH)),
                                    output_field=models.TextField())
                            ),
                        ],
                        [
                            (
                                filedname, Cast(
                                    Value(getattr(d, filedname)),
                                    output_field=models.TextField()
                                ),
                            )
                            for filedname in annotations_fields
                        ]
                    )
                )
                for d in querydata
            ],
            empty_annotations=list(chain([QUERY_DEPTH_FIELDNAME], annotations_fields)),
            sequence_name=None,
        )

    def _sort_query(
            self, *args,
            sequence_field=None,
            path_filedname=QUERY_PATH_FIELDNAME_FORMAT % {'name': 'sort', },
            prefetched=None,
    ):
        _sequence_field = sequence_field if sequence_field else F('id')
        node_model = self.model.get_node_model()

        def child_values(roots, nodedata, prefetch=False):
            for f in roots:
                base_path = getattr(f, path_filedname)
                base_ref_path = getattr(f, QUERY_NODE_PATH)
                depth = getattr(f, QUERY_DEPTH_FIELDNAME) + 1

                if base_ref_path in nodedata.keys():
                    yield f
                elif self.Pad(
                    f.pk,
                    padding_size=self.path_padding_size,
                    padding_char=self.path_padding_char,
                    sql=False
                ) in nodedata.keys():
                    yield f

                if prefetch:
                    children = []
                    for child in f.children.annotate(
                        **{
                            'path_parent_ref': Value(
                                int(f.pk), output_field=models.IntegerField()),
                            path_filedname: Concat(
                                Value(base_path + self._path_seperator),
                                self.Pad(_sequence_field),
                            ),
                            QUERY_NODE_PATH: Concat(
                                Value(base_ref_path + self.path_seperator),
                                self.Pad(
                                    F('id'),
                                    # NOTE: these use class default size as we need consultancy
                                    # incase we need to link calls to _sort_query
                                    padding_size=self.path_padding_size,
                                    padding_char=self.path_padding_char,
                                ),
                            ),
                            QUERY_DEPTH_FIELDNAME: Cast(
                                Value(depth),
                                output_field=models.IntegerField()
                            ),
                        }
                    ):
                        oldchild = nodedata.get(getattr(child, QUERY_NODE_PATH))
                        setattr(oldchild, path_filedname, getattr(child, path_filedname))
                        children.append(oldchild)
                    yield from child_values(
                        children, nodedata,
                        prefetch=prefetch,
                    )
                else:
                    yield from child_values(
                        f.children.annotate(
                            **{
                                'path_parent_ref': Value(
                                    int(f.pk), output_field=models.IntegerField()),
                                path_filedname: Concat(
                                    Value(base_path + self._path_seperator),
                                    self.Pad(_sequence_field),
                                ),
                                QUERY_NODE_PATH: Concat(
                                    Value(base_ref_path + self.path_seperator),
                                    self.Pad(
                                        F('id'),
                                        padding_size=self.path_padding_size,
                                        padding_char=self.path_padding_char
                                    ),
                                ),
                                QUERY_DEPTH_FIELDNAME: Cast(
                                    Value(depth),
                                    output_field=models.IntegerField()
                                ),
                            }
                        ), nodedata, prefetch=prefetch)

        if prefetched:
            # IF we are useing prefetched data we need to use the 'node-path' to fetch by
            query_nodedata = dict(map(
                lambda x: (getattr(x, QUERY_NODE_PATH), x),
                prefetched))
            search_roots = []
            for node in prefetched:
                if getattr(node, QUERY_DEPTH_FIELDNAME) == 0:
                    setattr(node, path_filedname, self.Pad(node.id, sql=False))
                    search_roots.append(node)
            annotations_fields = [
                key
                for key in self.query.annotations.keys()
                if key not in [
                    QUERY_DEPTH_FIELDNAME,
                    QUERY_NODE_PATH
                ]
            ]
        else:
            query_nodedata = dict(map(
                lambda x: (self.Pad(
                        x.pk,
                        padding_size=self.path_padding_size,
                        padding_char=self.path_padding_char,
                        sql=False
                    ), x),
                self))
            search_roots = node_model.objects.roots().annotate(**{
                path_filedname: self.Pad(F('id')),
                QUERY_NODE_PATH: self.Pad(
                    F('id'),
                    padding_size=self.path_padding_size,
                    padding_char=self.path_padding_char
                ),
                QUERY_DEPTH_FIELDNAME: Cast(
                    Value(0),
                    output_field=models.IntegerField()
                )
            })
            annotations_fields = []

        annotations_fields.append(path_filedname)
        data = list(child_values(search_roots, query_nodedata, prefetch=bool(prefetched)))

        return (
            node_model,
            data,
            partial(self._build_query_fn, model=node_model, annotations_fields=annotations_fields)
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
        except NodeNotReachableException as err:  # noqa: F841
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
