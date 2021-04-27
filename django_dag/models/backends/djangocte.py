from django.db import models, connection
from django.core.exceptions import ValidationError
from django.db.models.functions import (
    Cast,
    Concat,
    Left,
    LPad,
    StrIndex,
    Substr,
    RowNumber
)
from django.db.models.expressions import (
    Case,
    ExpressionWrapper,
    F,
    Value,
    When,
    Window,
    Q,
)
from django.db.models import FilteredRelation
from django.db.models import Exists, OuterRef, Subquery
from django.db.models import Max
from django_dag.exceptions import NodeNotReachableException
from django_cte import CTEQuerySet, With
from .base import BaseNode

# replace broken CTEManager
class CTEManager(models.Manager):
    """Manager for models that perform CTE queries"""

    #def get_queryset(self):
    #    return (self.model, using=self._db)

    @classmethod
    def from_queryset(cls, queryset_class, class_name=None):
        assert issubclass(queryset_class, CTEQuerySet)
        return super().from_queryset(queryset_class, class_name=class_name)

ProtoNodeManager = CTEManager
ProtoEdgeManager = CTEManager
ProtoEdgeQuerySet = CTEQuerySet

QUERY_ORDER_FIELDNAME_FORMAT = 'dag_%(name)s_sequence'
QUERY_PATH_FIELDNAME_FORMAT = 'dag_%(name)s_path'
QUERY_DEPTH_FIELDNAME = 'dag_depth'
_PATH_PADDING_SIZE = 4

class ProtoNodeQuerySet(CTEQuerySet):
    def _LPad(self, value, padsize):
        return LPad(
            Cast(value, output_field=models.TextField()),
            padsize, Value('0'))

    def with_top_down(self, *args, padsize=_PATH_PADDING_SIZE ,**kwargs):
        """
        Generates a query that does a top-to-bottom traversal without regard to any
        possible (left-to-right) ordering of the nodes

        :param padsize int: Length of the field segment for each node in pits path
            to it root.
        """
        return self._sort_query(*args,
            padsize=padsize, sort_name='top_down' ,**kwargs)

    def with_depth_first(self, *args, padsize=_PATH_PADDING_SIZE ,**kwargs):
        """
        Generates a query that does a depth-first traversal, this account for the nodes
        sequence ordering (left-to-right) of the nodes.

        :param padsize int: Length of the field segment for each node in pits path
            to it root.
        """
        if self.model.sequence_manager:
            sequence_field=self.model.sequence_manager \
                .get_edge_rel_sort_query_component(
                    self.model, 'child_id', 'parent_id'
                )
        return self._sort_query(*args,
            padsize=padsize, sort_name='depth_first',
            sequence_field=sequence_field, **kwargs)

    def _sort_query(self, *args, padsize,
            sort_name='sort', sequence_field=None):
        path_filedname = QUERY_PATH_FIELDNAME_FORMAT % { 'name': sort_name,}
        node_model = self.model
        edge_model = self.model.get_edge_model()
        result_model = edge_model
        node_paths_cte = With.recursive(
            self._make_path_src_cte_fn(
                node_model, self.roots(),
                sequence_field if sequence_field else F('child_id'),
                padsize
            ),
            name = 'nodePaths'
        )
        subnodes = node_paths_cte.join(
                node_model,
                id=node_paths_cte.col.cid,
            ) \
            .with_cte(node_paths_cte) \
            .annotate(**{
                path_filedname:node_paths_cte.col.path,
                QUERY_DEPTH_FIELDNAME: node_paths_cte.col.depth,
            })
        roots = self.roots() \
            .annotate(**{
                path_filedname:self._LPad(F('id'), padsize),
                QUERY_DEPTH_FIELDNAME: Value(0, output_field=models.IntegerField()),
            })
        return subnodes.union(roots)

    def _breath_first(self, ):
        pass

    def _make_path_src_cte_fn(self, model, values, sequence_field, padsize):
        return model._base_tree_cte_builder(
                'parent_id',
                'cid',
                {
                    'eid': F('id'),
                    'cid':F('child_id'),
                    'pid':F('parent_id'),
                },
                {
                    'path': Concat(
                        self._LPad(F('parent_id'), padsize),
                        Value(","),
                        self._LPad(sequence_field, padsize)
                    ),
                    'depth':Value(1, output_field=models.IntegerField())
                },
                (lambda cte: {
                    'path': Concat(
                                cte.col.path, Value(","), self._LPad(sequence_field, padsize),
                                output_field=models.TextField(),),
                    'depth':cte.col.depth + Value(1, output_field=models.IntegerField())
                    }
                ),
                {'parent__in': values}
        )


class ProtoNode(BaseNode):
    def make_related_cte_fn(self, remote_name, local_name):
        return self._base_tree_cte_builder(
                local_name,'nid',
                {'nid':F(remote_name),},
                {'depth':Value(1, output_field=models.IntegerField()) },
                (lambda cte: {'depth': cte.col.depth + Value(1, output_field=models.IntegerField()) }),
                {local_name:self.pk}
        )

    @property
    def descendants(self):
        return self._descendants_query()

    def get_descendant_pks(self):
        return [
            row
            for row in self._descendants_query().values_list('pk', flat=True)
        ]

    def _descendants_query(self):
        node_model = self.get_node_model()
        cte = With.recursive(self.make_related_cte_fn(
            remote_name='child_id', local_name='parent_id'
        ))
        return cte.join(node_model, id=cte.col.nid) \
            .with_cte(cte) \
            .annotate(depth=Max(cte.col.depth)) \
            .order_by('id', 'depth')

    @property
    def ancestors(self):
        return self._ancestors_query()

    def get_ancestor_pks(self):
        return [
            row
            for row in self._ancestors_query().values_list('pk', flat=True)
        ]

    def _ancestors_query(self):
        node_model = self.get_node_model()
        cte = With.recursive(self.make_related_cte_fn(
            remote_name='parent_id', local_name='child_id'
        ))
        return cte.join(node_model, id=cte.col.nid) \
            .with_cte(cte)\
            .annotate(depth=Max(cte.col.depth)) \
            .order_by('id', 'depth') 

    @property
    def clan(self):
        # NOTE: This is less then ideall as ATM you cannot join with (or |) cte queries
        ancestors=list(self.ancestors.values_list('pk',flat=True))
        descendants=list(self.descendants.values_list('pk',flat=True))
        return self.get_node_model().objects.filter(pk__in=[self.pk]+ancestors+descendants)

    @classmethod
    def _base_tree_cte_builder(cls, local_name, link_name, result_spec,
                recurse_init_spec, recurse_next_spec, initial_filter_spec):

        # Since we have a recurse use for CTE, and  tree srutue all our cte's look
        # similar  we walk from localname on across to link name, making a union
        # so the values in results table look similar (result_spec) it jus the 
        # initial quiery needs to seen some values (recurse_init) and recurse_next
        # contains optional calcs.

        def cte_builder(cte):
            recurse_init_values = recurse_init_spec(cte) if callable(recurse_init_spec) else dict(recurse_init_spec)
            recurse_init_values.update(result_spec)

            recurse_next_values = recurse_next_spec(cte) if callable(recurse_next_spec) else dict(recurse_next_spec)
            recurse_next_values.update(result_spec)

            initial_filter = initial_filter_spec(cte) if callable(initial_filter_spec) else dict(initial_filter_spec)
            edge_model = cls.get_edge_model()
            basic_cte_query = ( edge_model.objects.filter(**initial_filter)
                .values( **recurse_init_values)
                .union(
                    cte.join(edge_model, **{local_name: getattr(cte.col,link_name)})
                    .values(**recurse_next_values)
                    .distinct(),
                    all = False
                ))
            return basic_cte_query
        return cte_builder

    def make_root_leaf_cte_fn(self, remote_name, local_name):
        return self._base_tree_cte_builder(
                local_name,'rid',
                {'rid':F(remote_name),'lid':F(local_name),},
                {},{},
                {local_name:self.pk}
        )
    def get_roots(self):
        return self._get_source_sink_node('parent_id', 'child_id')

    def get_leaves(self):
        return self._get_source_sink_node('child_id', 'parent_id')

    def _get_source_sink_node(self,remote_name, local_name):
        edge_model = self.get_edge_model()
        node_model = self.get_node_model()
        cte = With.recursive(self.make_root_leaf_cte_fn(
            remote_name=remote_name, local_name=local_name
        ))
        datarows = cte.join(node_model, pk=cte.col.rid) \
            .with_cte(cte) \
            .annotate(
                linked = Exists(
                    edge_model.objects.filter(**{local_name: OuterRef('pk')})
                )
            ) \
            .filter(linked=False) \
            .union(
                node_model.objects.filter(pk=self.pk) \
                .annotate(
                    linked = Exists(
                        edge_model.objects.filter(**{local_name: OuterRef('pk')})
                    )
                ) \
                .filter(linked=False)
            )
        return datarows

    def make_path_src_cte_fn(self, field_name, target):
        return self._base_tree_cte_builder(
                'parent_id','cid',
                {'cid':F('child_id'),'pid':F('parent_id'),},
                {'path':F(field_name),'depth':Value(1, output_field=models.IntegerField())},
                (lambda cte: {'path': Concat(
                                cte.col.path, Value(","), F(field_name),
                                output_field=models.TextField(),),
                            'depth':cte.col.depth + Value(1, output_field=models.IntegerField())
                            }),
                {'parent_id': self.pk}
        )

    def get_paths(self, target, use_edges=False, downwards=None):
        try:
            if downwards is None or downwards is True:
                return list(self._get_path_edge_cte(target, use_edges=use_edges, downwards=True))
        except NodeNotReachableException as err:
            if downwards is True:
                raise
        return list(self._get_path_edge_cte(target, use_edges=use_edges, downwards=False))

    def _get_path_edge_cte(self, target, use_edges=False, downwards=True):
        if self == target:
            # There can only be 1 zero length path, it also has no edge
            # so we can always return [] for the path
            yield []
            return

        node_model = self.get_node_model()
        edge_model = self.get_edge_model()
        result_model = edge_model if use_edges else node_model

        if downwards:
            source = self
            target = target
            result = target
            element = edge_model._meta.pk.name if use_edges else 'child_id'
        else:
            source = target
            target = self
            result = self
            element = edge_model._meta.pk.name if use_edges else 'parent_id'

        node_paths_cte = With.recursive(
            source.make_path_src_cte_fn(element, target),
            name = 'nodePaths'
        )

        def qFilter(cte):
            return cte.queryset() \
                .filter(cid = result.pk)

        orderlists_cte = With.recursive(
            self.make_list_items_cte_fn(
                node_paths_cte,
                filter_fn=qFilter,
                list_col=node_paths_cte.col.path
            ),
            name = 'orderedListItems'
        )

        datarows = orderlists_cte.join(result_model, id=orderlists_cte.col.item_id) \
            .with_cte(node_paths_cte) \
            .with_cte(orderlists_cte) \
            .annotate(
                item_order=orderlists_cte.col.item_order,
                item_group=orderlists_cte.col.item_group,
            ) \
            .order_by('item_group', 'item_order')
                #.order_by(node_paths_cte.col.depth, node_paths_cte.col.cid)

        # Convert to list groups
        group = None
        values = None
        for item in datarows:
            if values is None:
                values = []
                group = item.item_group
            if group == item.item_group:
                values.append(item)
            else:
                group = item.item_group
                if values is not None: yield values
                values=[item]
        if values is not None:
            yield values
            return
        raise NodeNotReachableException()

    @classmethod
    def make_list_items_cte_fn(self, query, filter_fn, list_col):
        def make_list_items_cte(cte):
            basic_cte_query = filter_fn(query)
            result_cte_query =  basic_cte_query\
                .annotate(
                    safelist = Concat(
                        list_col, Value(","),
                        output_field=models.TextField(),
                    )
                ) \
                .values(
                    item_id = Substr(
                        F('safelist'), 1, StrIndex(F('safelist'), Value(','),) -1,
                        output_field=models.IntegerField()
                    ),
                    node_path = Substr(
                        F('safelist'), StrIndex(F('safelist'), Value(','),) + 1,
                        output_field=models.TextField(),
                    ),
                    item_group = list_col,
                    item_order = Value(0, output_field=models.IntegerField())
                ) \
                .union(
                    cte.join(basic_cte_query, path=cte.col.item_group)\
                        .annotate(
                            remainder = ExpressionWrapper(
                                cte.col.node_path,
                                output_field=models.TextField(),
                            ),
                        ) \
                        .filter(remainder__gt = "") \
                        .values(
                            item_id = Substr(
                                cte.col.node_path, 1, StrIndex(cte.col.node_path, Value(','),) -1,
                                output_field=models.IntegerField()
                            ),
                            node_path = Substr(
                                cte.col.node_path, StrIndex(cte.col.node_path, Value(','),) + 1,
                                output_field=models.TextField(),
                            ),
                            item_group = cte.col.item_group,
                            item_order =  Value(1, output_field=models.IntegerField()) + cte.col.item_order
                        ),
                        all=True,
                )
            return result_cte_query
        return make_list_items_cte

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
