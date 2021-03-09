from django.db import models, connection
from django.core.exceptions import ValidationError
from django.db.models.functions import (
    Concat,
    Left,
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
from django_cte import CTEManager, With
from .standard import ProtoNode as bProtoNode

ProtoNodeManager = CTEManager
ProtoEdgeManager = CTEManager


class ProtoNode(bProtoNode):
    def make_related_cte_fn(self, remote_name, local_name):
        def make_related_cte(cte):
            edge_model = self.get_edge_model()
            basic_cte_query = edge_model.objects.filter(**{local_name:self.pk}) \
                .values(
                    nid=F(remote_name),
                    depth=Value(1, output_field=models.IntegerField())
                ) \
                .union(
                    cte.join(edge_model, **{local_name:cte.col.nid}) \
                        .values(
                            nid=F(remote_name),
                            depth=cte.col.depth + Value(1, output_field=models.IntegerField())
                        ).distinct(),
                        all=False,
                )
            return basic_cte_query
        return make_related_cte

    @property
    def descendants(self):
        return [row for row in self._descendants_query()]

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
            .with_cte(cte).order_by('id', 'depth') \
            .annotate(depth=Max(cte.col.depth))

    @property
    def ancestors(self):
        return [row for row in self._ancestors_query()]

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
            .with_cte(cte).order_by('id', 'depth') \
            .annotate(depth=Max(cte.col.depth))

    def make_root_leaf_cte_fn(self, remote_name, local_name):
        def make_related_cte(cte):
            edge_model = self.get_edge_model()
            basic_cte_query = edge_model.objects.filter(**{local_name:self.pk}) \
                .values(
                    rid=F(remote_name),
                    lid=F(local_name),
                ) \
                .union(
                    cte.join(edge_model, **{local_name:cte.col.rid}) \
                        .values(
                            rid=F(remote_name),
                            lid=F(local_name),
                        ).distinct(),
                        all=False,
                )
            return basic_cte_query
        return make_related_cte

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
            ) \
            .order_by('id')
        return datarows

    def make_path_cte_fn(self, field_name, source, target):
        def make_path_cte(paths):
            edge_model = self.get_edge_model()
            basic_paths_cte_query = edge_model.objects.filter(parent_id=source.pk) \
                .values(
                    cid=F('child_id'),
                    pid=F('parent_id'),
                    path=F(field_name),
                    depth=Value(1, output_field=models.IntegerField())
                ) \
                .union(
                    paths.join(
                            edge_model.objects.filter(~Q(parent_id=target.pk)),
                            parent_id=paths.col.cid
                        ) \
                        .values(
                            cid=F('child_id'),
                            pid=F('parent_id'),
                            path=Concat(
                                paths.col.path, Value(","), F(field_name),
                                output_field=models.TextField(),
                            ),
                            depth=paths.col.depth + Value(1, output_field=models.IntegerField())
                        ).distinct(),
                        all=False,
                )
            return basic_paths_cte_query
        return make_path_cte

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
            self.make_path_cte_fn(
                field_name=element,
                source=source,
                target=target
            ),
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
