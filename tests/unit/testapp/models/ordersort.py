# -*- coding: utf-8 -*-
from django.db import models
from django.utils.translation import ugettext_lazy as _
from django.db.models import OuterRef, Subquery
from django_dag.models.order_control import (
    BaseDagNodeOrderController,
    BaseDagEdgeOrderController
)

class DagNodeIntSorter(BaseDagNodeOrderController):
    """Simple node sorter based on Broken Integer implementation"""
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

    def key_between(self, instance, other, parent):
        """
        Return a key half way between this and other - assuming no other
        intermediate keys exist in the tree.
        """
        return int(instance.sequence + (other.sequence - instance.sequence)/2)

    def next_key(self, instance, parent):
        """
        Provide the next key in the sequence
        """
        return instance.sequence + (100 - instance.sequence) /2

    def initial_key(self,):
        """
        Provide the first key in the sequence
        """
        return 50

    def prev_key(self, instance, parent):
        """
        Provide the first key in the sequence
        """
        return instance.sequence - instance.sequence / 2


class DagEdgeIntSorter(BaseDagEdgeOrderController):
    """Simple edge sorter based on Broken Integer implementation"""

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
        ## Our process here is to
        # Find edges which come off the base's parent node,
        # and are numbered higher than our found base to parent link.
        # These are then order and the first one by sequne selected.
        # Note: assumes only one edges is possible, or raise MultipleObjectsReturned
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
        # Return the endpoint of the selected edge iff exists.
        return sibling_node_edge.child if sibling_node_edge else None

    def get_prev_sibling(self, basenode, parent_node):
        ## See above for breakdown of quesry
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

    def key_between(self, instance, other, parent):
        """
        Return a key half way between this and other - assuming no other
        intermediate keys exist in the tree.
        """
        edges = instance.children.through.objects.filter(
            parent=parent,
            child__in=[ instance, other]
        ).order_by(self.sequence_field_name).values_list(self.sequence_field_name, flat=True)
        assert len(edges) == 2, "We only support one noe connecting parent to child"
        return int(edges[0] + (edges[1] - edges[0])/2)

    def next_key(self, instance, parent):
        """
        Provide the next key in the sequence
        """
        edges = instance.children.through.objects.filter(
            parent=parent,
            child=instance
        ).values_list(self.sequence_field_name, flat=True)
        return edges[0] + (100 - edges[0]) /2

    def initial_key(self,):
        """
        Provide the first key in the sequence
        """
        return 50

    def prev_key(self, instance, parent):
        """
        Provide the first key in the sequence
        """
        edges = instance.children.through.objects.filter(
            parent=parent,
            child=instance
        ).values_list(self.sequence_field_name, flat=True)
        return edges[0] - edges[0] /2
