from abc import ABC, abstractmethod
from typing import List
from django.db import models, NotSupportedError
from django.db.models.functions import (
    Cast,
    Concat,
    LPad,
    StrIndex,
    Substr,
    RowNumber
)
from django.db.models.expressions import (
    ExpressionWrapper,
    F,
    Value
)
from django.db.models.query import EmptyQuerySet, QuerySet
from django.db.models import (
    Exists,
    Max,
    OuterRef,
    Subquery,
    Window
)
from django_dag.exceptions import NodeNotReachableException
from django_cte import __version__ as cte_version, CTEQuerySet, With
from django_delayed_union.base import DelayedQuerySetMethod
from .base import BaseNode
from .query import DagBaseDelayedUnionQuerySet
from . import (
    QUERY_PATH_FIELDNAME_FORMAT,
    QUERY_DEPTH_FIELDNAME,
    QUERY_NODE_PATH,
)


if cte_version < "1.1.6":
    # NOTE: replace broken CTEManager
    class CTEManager(models.Manager):
        """Manager for models that perform CTE queries"""

        @classmethod
        def from_queryset(cls, queryset_class, class_name=None):
            assert issubclass(queryset_class, CTEQuerySet)
            return super().from_queryset(queryset_class, class_name=class_name)
else:
    from django_cte import CTEManager


ProtoNodeManager = CTEManager
ProtoEdgeManager = CTEManager
ProtoEdgeQuerySet = CTEQuerySet


class SplitPassthroughMethod(DelayedQuerySetMethod):
    """
    A modified 'Django Delayed Union' Passthrough field to support
    dags queried with a root and child node parts
    """
    def __call__(self, obj, *args, **kwargs):
        assert len(obj._querysets) == 2, "Can only work on DAG querysets"
        main, roots = obj._querysets[:2]
        assert kwargs.pop('roots', None) is None, "duplicate root"
        main_clone = main._clone()
        roots_clone = roots._clone()

        return obj._clone([
            getattr(main_clone, self.name)(*args, roots=roots_clone, **kwargs)
        ])

    def get_base_docstring(self):
        return """
        Returns a new delayed queryset with `{name}(...)`` having been called
        on each of the component querysets.:
        """


class DagDelayedUnionQuerySet(DagBaseDelayedUnionQuerySet):
    with_sort_sequence = SplitPassthroughMethod()
    distinct_node = SplitPassthroughMethod()


class DagCteAnnotation(ABC):
    @abstractmethod
    def as_initial_expresion(self, cte):
        """
        Convert to tuple (name, Expression) which forms the annotation for
        the initial query in the CTE

        :rtype: tuple( str, Union[`django.db.models.Expression`, Function:->`django.db.models.Expression`])
        :return: tuple of
            * Name of annotation or CTE table column
            * Expression forming the annotation
        """
        pass

    @abstractmethod
    def as_recursive_expresion(self, cte):
        """
        Convert to tuple (name, Expression) which forms the annotation for
        the recursive part of query in the CTE

        :rtype: tuple( str, Union[`django.db.models.Expression`, Function:->`django.db.models.Expression`])
        :return: tuple of
            * Name of annotation or CTE table column
            * Expression forming the annotation
        """
        pass


class CteRawAnnotation(DagCteAnnotation):
    """
    A simple DagCteAnnotation where the initial and recursive part are raw expressions
    or functions returning expressions.
    """
    def __init__(self, name, initial, recursive) -> None:
        self.name = name
        self.initial = initial
        self.recursive = recursive

    def as_initial_expresion(self, cte):
        if callable(self.initial):
            return (self.name, self.initial(cte))
        return (self.name, self.initial)

    def as_recursive_expresion(self, cte):
        if callable(self.recursive):
            return (self.name, self.recursive(cte))
        return (self.name, self.recursive)


class CteSimpleConcatAnnotation(DagCteAnnotation):
    def __init__(
            self,
            name: str,
            initial_sequence_field,
            next_sequence_field,
            path_seperator: str,
            padding_size: int,
            padding_char: str,
    ) -> None:
        """
        :param name: Name of the annotation  or CTE table column
        :param initial_sequence_field: An expression or F() used to from the initial value
            of the path
        :param next_sequence_field: An expression or F() used to get the next part of the
            combined path field.
        :param path_seperator (str): Character to put between fields
        :param padding_size (int): The number of characters that the field should be when padded
        :param padding (str): character to pad the field
        """
        self.name = name
        self.initial_sequence_field = initial_sequence_field
        self.next_sequence_field = next_sequence_field
        self.path_seperator = path_seperator
        self.padding_size = padding_size
        self.padding_char = padding_char

    def _LPad(self, value):
        return LPad(
            Cast(value, output_field=models.TextField()),
            self.padding_size, Value(self.padding_char))

    def as_initial_expresion(self, cte):
        return (
            self.name,
            Concat(
                self._LPad(self.initial_sequence_field),
                Value(self.path_seperator),
                self._LPad(self.next_sequence_field)
            )
        )

    def as_recursive_expresion(self, cte):
        return (
            self.name,
            Concat(
                getattr(cte.col, self.name),
                Value(self.path_seperator),
                self._LPad(self.next_sequence_field),
                output_field=models.TextField(),)
        )


class ProtoNodeQuerySet(CTEQuerySet):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _LPad(self, value, padding_size=None, padding_char=None):
        padding_size = padding_size or self._padding_size
        padding_char = padding_char or self._padding_char
        return LPad(
            Cast(value, output_field=models.TextField()),
            padding_size, Value(padding_char))

    def distinct_node(self, order_field: str, roots: QuerySet = None, **kwargs):
        """
        Modifies the query so that with nodes as distinct.

        As withing the DAG node can occur a number of times, this add support for a
        topological like view of the dag, where only the first visit to a node is included.
        This must be called after the initial query to add the order clause is run

        :param order_field (str): The name of the field or annotation the is used to
            order the 'distinct' nodes to determin the first
        """
        if roots is None:
            raise NotSupportedError(
                'Cannot apply node distinction until a node sequence is applied')

        # This is used to filter the nodes so we only select the first visit
        # to a node. It is only needed to be done on the non root nodes
        # as root node will always be visited only once.
        # As SQL cannot filter on a window function we use a subquery via
        # a with statement to preform this
        nodes_cte = With(
            self.annotate(
                _visit_count=Window(
                    expression=RowNumber(),
                    partition_by=[F('id'), ],
                    order_by=F(order_field).asc(),
                )
            ),
            name=f'distinct_node_{order_field}'
        )
        results = nodes_cte \
            .queryset() \
            .with_cte(nodes_cte) \
            .filter(
                _visit_count=1
            )
        roots = roots.annotate(_visit_count=Value(1, output_field=models.IntegerField()))
        return DagDelayedUnionQuerySet(results, roots)

    def with_pk_path(self, *args, name=None, **kwargs):
        """
        Generates a query that does a top-to-bottom traversal without regard to any
        possible (left-to-right) ordering of the nodes
        """
        if name is None:
            name = QUERY_PATH_FIELDNAME_FORMAT % {'name': 'pk', }

        return DagDelayedUnionQuerySet(
                *self._sort_query(
                    *args,
                    path_filedname=name,
                    **kwargs))

    def with_sequence_path(self, *args, name=None, **kwargs):
        """
        Generates a query that add annotations for depth-first traversal to the nodes

        This account for the nodes sequence ordering (left-to-right) of the nodes and
        allows the nodes to be sorted.
        nodes.orderby('dag_sequence_path') produces as 'preorder (Root, Left, Right)' sort

        Nodes with multiple roots will be present in the results multiple time
        """
        sequence_field = None
        if self.model.sequence_manager:
            sequence_field = self.model.sequence_manager \
                .get_edge_rel_sort_query_component(
                    self.model, 'child_id', 'parent_id'
                )
        if name is None:
            name = QUERY_PATH_FIELDNAME_FORMAT % {'name': 'sequence', }

        return DagDelayedUnionQuerySet(
            *self._sort_query(
                *args,
                path_filedname=name,
                sequence_field=sequence_field,
                **kwargs))

    def _sort_query(
            self, *args,
            sequence_field=None,
            path_filedname=QUERY_PATH_FIELDNAME_FORMAT % {'name': 'sort', },
            roots=None,
    ):
        node_model = self.model.get_node_model()
        if isinstance(self, EmptyQuerySet):
            self.annotate(**{
                path_filedname: Value(None, output_field=models.IntegerField()),
                QUERY_DEPTH_FIELDNAME: Value(None, output_field=models.IntegerField()),
            })

        if roots:
            search_roots = self.query.model.objects.filter(
                pk__in=Subquery(roots.model.objects.roots().values("pk"))
            )
        else:
            search_roots = self.query.model.objects.filter(
                    pk__in=Subquery(
                        self.query.model.objects.roots().values("pk")
                    )
            )

        node_paths_cte = With.recursive(
            self._make_path_src_cte_fn(
                node_model,
                search_roots,
                [
                    CteSimpleConcatAnnotation(
                        'querypath',
                        F('parent_id'),
                        sequence_field if sequence_field else F('child_id'),
                        path_seperator=self._path_seperator,
                        padding_size=self._padding_size,
                        padding_char=self._padding_char,
                    ),
                ],
            ),
            name='node_paths' + path_filedname
        )

        joins = {
            'id': node_paths_cte.col.cid
        }
        if roots:
            joins[QUERY_NODE_PATH] = node_paths_cte.col.path

        subnodes = node_paths_cte.join(self, **joins) \
            .with_cte(node_paths_cte) \
            .annotate(**{
                path_filedname: node_paths_cte.col.querypath,
                QUERY_NODE_PATH: node_paths_cte.col.path,
                QUERY_DEPTH_FIELDNAME: node_paths_cte.col.depth,
            })

        if roots is None:
            roots = self.roots()

        roots = roots \
            .annotate(**{
                path_filedname: self._LPad(F('id')),
                QUERY_NODE_PATH: self._LPad(
                    F('id'),
                    # NOTE: these use class default size as we need consultancy
                    # incase we need to link calls to _sort_query
                    padding_size=self.path_padding_size,
                    padding_char=self.path_padding_char
                ),
                QUERY_DEPTH_FIELDNAME: Value(0, output_field=models.IntegerField()),
            })
        return subnodes, roots

    def _make_path_src_cte_fn(self, model, rootquery,
            sequence_fields: List[DagCteAnnotation]):
        """
        Build the CTE query function for dag path navigation

        :param model:
        :param rootquery: ids for root nodes
        :param sequence_fields: List<DagCteAnnotation> to form the CTE
        """
        annotations = sequence_fields.copy()
        annotations.extend([
            CteSimpleConcatAnnotation(
                'path',
                F('parent_id'),
                F('child_id'),
                path_seperator=self.path_seperator,
                padding_size=self.path_padding_size,
                padding_char=self.path_padding_char
            ),
            CteRawAnnotation(
                'depth',
                Value(1, output_field=models.IntegerField()),
                lambda cte: cte.col.depth + Value(1, output_field=models.IntegerField()),
            )
        ])
        return model._base_tree_cte_builder(
            'parent_id',
            'cid',
            {
                'eid': F('id'),
                'cid': F('child_id'),
                'pid': F('parent_id'),
            },
            (lambda cte: dict(map(lambda field: field.as_initial_expresion(cte), annotations))),
            (lambda cte: dict(map(lambda field: field.as_recursive_expresion(cte), annotations))),
            {'parent__in': rootquery}
        )


class ProtoNode(BaseNode):
    def make_related_cte_fn(self, remote_name, local_name):
        return self._base_tree_cte_builder(
            local_name, 'nid',
            {'nid': F(remote_name), },
            {'depth': Value(1, output_field=models.IntegerField())},
            (lambda cte: {'depth': cte.col.depth +
             Value(1, output_field=models.IntegerField())}),
            {local_name: self.pk}
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
        ancestors = list(self.ancestors.values_list('pk', flat=True))
        descendants = list(self.descendants.values_list('pk', flat=True))
        return self.get_node_model().objects.filter(pk__in=[self.pk] + ancestors + descendants)

    @classmethod
    def _base_tree_cte_builder(cls, local_name, link_name, result_spec,
                recurse_init_spec, recurse_next_spec, initial_filter_spec):

        # Since we have a recurse use for CTE, and  tree structure all our CTE's look
        # similar  we walk from localname on across to link name, making a union
        # so the values in results table look similar (result_spec) it jus the
        # initial query needs to seen some values (recurse_init) and recurse_next
        # contains optional calcs.

        def cte_builder(cte):
            recurse_init_values = recurse_init_spec(cte) if callable(
                recurse_init_spec) else dict(recurse_init_spec)
            recurse_init_values.update(result_spec)

            recurse_next_values = recurse_next_spec(cte) if callable(
                recurse_next_spec) else dict(recurse_next_spec)
            recurse_next_values.update(result_spec)

            initial_filter = initial_filter_spec(cte) if callable(
                initial_filter_spec) else dict(initial_filter_spec)
            edge_model = cls.get_edge_model()
            basic_cte_query = (edge_model.objects.filter(**initial_filter)
                               .values(**recurse_init_values)
                               .union(
                cte.join(edge_model, **
                         {local_name: getattr(cte.col, link_name)})
                .values(**recurse_next_values)
                .distinct(),
                all=False
            ))
            return basic_cte_query
        return cte_builder

    def make_root_leaf_cte_fn(self, remote_name, local_name):
        return self._base_tree_cte_builder(
            local_name, 'rid',
            {'rid': F(remote_name), 'lid': F(local_name), },
            {}, {},
            {local_name: self.pk}
        )

    def get_roots(self):
        return self._get_source_sink_node('parent_id', 'child_id')

    def get_leaves(self):
        return self._get_source_sink_node('child_id', 'parent_id')

    def _get_source_sink_node(self, remote_name, local_name):
        edge_model = self.get_edge_model()
        node_model = self.get_node_model()
        cte = With.recursive(self.make_root_leaf_cte_fn(
            remote_name=remote_name, local_name=local_name
        ))
        datarows = cte.join(node_model, pk=cte.col.rid) \
            .with_cte(cte) \
            .annotate(
                linked=Exists(
                    edge_model.objects.filter(**{local_name: OuterRef('pk')})
                )
        ) \
            .filter(linked=False) \
            .union(
                node_model.objects.filter(pk=self.pk)
                .annotate(
                    linked=Exists(
                        edge_model.objects.filter(
                            **{local_name: OuterRef('pk')})
                    )
                )
                .filter(linked=False)
        )
        return datarows

    def make_path_src_cte_fn(self, field_name, target):
        return self._base_tree_cte_builder(
            'parent_id', 'cid',
            {'cid': F('child_id'), 'pid': F('parent_id'), },
            {'path': F(field_name), 'depth': Value(
                1, output_field=models.IntegerField())},
            (lambda cte: {'path': Concat(
                cte.col.path, Value(","), F(field_name),
                output_field=models.TextField(),),
                'depth': cte.col.depth + Value(1, output_field=models.IntegerField())
            }),
            {'parent_id': self.pk}
        )

    def get_paths(self, target, use_edges=False, downwards=None):
        try:
            if downwards is None or downwards is True:
                return list(self._get_path_edge_cte(target, use_edges=use_edges, downwards=True))
        except NodeNotReachableException as err:  # noqa: F841
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
            name='node_paths'
        )

        def qFilter(cte):
            return cte.queryset() \
                .filter(cid=result.pk)

        orderlists_cte = With.recursive(
            self.make_list_items_cte_fn(
                node_paths_cte,
                filter_fn=qFilter,
                list_col=node_paths_cte.col.path
            ),
            name='ordered_list_items'
        )

        datarows = orderlists_cte.join(result_model, id=orderlists_cte.col.item_id) \
            .with_cte(node_paths_cte) \
            .with_cte(orderlists_cte) \
            .annotate(
                item_order=orderlists_cte.col.item_order,
                item_group=orderlists_cte.col.item_group,
        ) \
            .order_by('item_group', 'item_order')
        # .order_by(node_paths_cte.col.depth, node_paths_cte.col.cid)

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
                if values is not None:
                    yield values
                values = [item]
        if values is not None:
            yield values
            return
        raise NodeNotReachableException()

    @classmethod
    def make_list_items_cte_fn(self, query, filter_fn, list_col):
        def make_list_items_cte(cte):
            basic_cte_query = filter_fn(query)
            result_cte_query = basic_cte_query\
                .annotate(
                    safelist=Concat(
                        list_col, Value(","),
                        output_field=models.TextField(),
                    )
                ) \
                .values(
                    item_id=Substr(
                        F('safelist'), 1, StrIndex(
                            F('safelist'), Value(','),) - 1,
                        output_field=models.IntegerField()
                    ),
                    node_path=Substr(
                        F('safelist'), StrIndex(F('safelist'), Value(','),) + 1,
                        output_field=models.TextField(),
                    ),
                    item_group=list_col,
                    item_order=Value(0, output_field=models.IntegerField())
                ) \
                .union(
                    cte.join(basic_cte_query, path=cte.col.item_group)
                    .annotate(
                        remainder=ExpressionWrapper(
                            cte.col.node_path,
                            output_field=models.TextField(),
                        ),
                    )
                    .filter(remainder__gt="")
                    .values(
                        item_id=Substr(
                            cte.col.node_path, 1, StrIndex(
                                cte.col.node_path, Value(','),) - 1,
                            output_field=models.IntegerField()
                        ),
                        node_path=Substr(
                            cte.col.node_path, StrIndex(
                                cte.col.node_path, Value(','),) + 1,
                            output_field=models.TextField(),
                        ),
                        item_group=cte.col.item_group,
                        item_order=Value(
                            1, output_field=models.IntegerField()) + cte.col.item_order
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
