# -*- coding: utf-8 -*-
from django.db import models
from django.utils.translation import ugettext_lazy as _
from django.db.models import OuterRef, Subquery
from django.db.models import Count, F, Value
from django.core.exceptions import ObjectDoesNotExist
from django_dag.models import node_factory, edge_factory
from django_dag.models.order_control import BaseDagOrderController



class DagNodeIntSorter(BaseDagOrderController):

    @classmethod
    def get_node_sequence_field(cls, ):
        """
        Returns a single field to be used to support ordering.
        Should the controller require more fields this can be a primary key
        to another model
        """
        return models.IntegerField(
                verbose_name=_("Node sequence"),
                db_index=True,
                default=0,
            )

    @classmethod
    def get_edge_sequence_field(cls, ):
        """
        Returns a single field to be used to support ordering.
        Should the controller require more fields this can be a primary key
        to another model
        """
        return None

    def get_relatedsort_query_component(self, model, targetname, sourcename):
        """
        Builds a query component that can be used for sorting a children of
        a dag Node.

        :return: django F() expressions
        """
        return F('sequence')
    def get_sorted_edge_queryset(self, node, target, source):
        edge_model = node.get_edge_model()
        return edge_model.objects.filter(**{
                source: node
            }).select_related(target).order_by(
                "%s__%s"%(target, self.sequence_field_name,)
            )

    def get_next_sibling(self, basenode, parent_node):
        edge_model = basenode.get_edge_model()
        sibling_node_edge = edge_model.objects.filter(
            parent=parent_node,
        ).filter(
            child__sequence__gt = basenode.sequence
        ).order_by(
            "child__%s"%(self.sequence_field_name,)
        ).select_related('child').first()
        return sibling_node_edge.child if sibling_node_edge else None

    def get_prev_sibling(self, basenode, parent_node):
        edge_model = basenode.get_edge_model()
        sibling_node_edge = edge_model.objects.filter(
            parent=parent_node,
        ).filter(
            child__sequence__lt = basenode.sequence
        ).order_by(
            "-child__%s"%(self.sequence_field_name,)
        ).select_related('child').first()
        return sibling_node_edge.child if sibling_node_edge else None


class DagEdgeIntSorter(BaseDagOrderController):
    """
    Simple sorter based on CharSortKey
    """

    @classmethod
    def get_node_sequence_field(cls, ):
        """
        Returns a single field to be used to support ordering.
        Should the controller require more fields this can be a primary key
        to another model
        """
        return None

    @classmethod
    def get_edge_sequence_field(cls, ):
        """
        Returns a single field to be used to support ordering.
        Should the controller require more fields this can be a primary key
        to another model
        """
        return models.IntegerField(
                verbose_name=_("Edge sequence"),
                db_index=True,
                blank=True,
            )

    def get_relatedsort_query_component(self, model, targetname, sourcename):
        """
        Builds a query component that can be used for sorting a children of
        a dag Node.

        :param model: A Node model or model instance
        :return: Subquery etc. which will result in a value for the node sequence
        """
        sequence = model.get_edge_model().objects
        if isinstance(model, models.Model):
            sequence = sequence.filter(**{sourcename:model})
        sequence = sequence.filter(
            **{targetname:OuterRef('pk')}
        )
        return Subquery(sequence.values(self.sequence_field_name))

    def get_sorted_edge_queryset(self, node, target, source):
        edge_model = node.get_edge_model()
        return edge_model.objects.filter(**{
                source: node
            }).select_related(target).order_by(self.sequence_field_name)

    def get_next_sibling(self, basenode, parent_node):
        edge_model = basenode.get_edge_model()
        try:
            sibling_node_edge = edge_model.objects.filter(
                parent=parent_node,
                sequence__gt = edge_model.objects.get(
                        parent=parent_node,
                        child=basenode
                    ).sequence
            ).order_by(self.sequence_field_name).select_related('child').first()
        except ObjectDoesNotExist:
            return None
        return sibling_node_edge.child if sibling_node_edge else None

    def get_prev_sibling(self, basenode, parent_node):
        edge_model = basenode.get_edge_model()
        try:
            sibling_node_edge = edge_model.objects.filter(
                parent=parent_node,
                sequence__lt = edge_model.objects.get(
                        parent=parent_node,
                        child=basenode
                    ).sequence
            ).order_by('-%s'%(self.sequence_field_name)).select_related('child').first()
        except ObjectDoesNotExist:
            return None
        return sibling_node_edge.child if sibling_node_edge else None
