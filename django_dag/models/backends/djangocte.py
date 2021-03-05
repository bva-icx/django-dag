from django.db import models, connection
from django.core.exceptions import ValidationError
from django.db.models import Case, When, Max, Value, F
from django.db.models import FilteredRelation, Q
from django.db.models import OuterRef, Subquery
from django.db.models import Exists
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

    def get_ancestors_pks(self):
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