# -*- coding: utf-8 -*-
from django.db import models
from django.utils.translation import ugettext_lazy as _
from django_dag.models import node_factory, edge_factory
from django_dag.models.order_control import BaseDagOrderController
from django.db.models import OuterRef, Subquery
from django.db.models import Count, F, Value


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

    def get_childsort_query_component(self, model):
        return self.get_relatedsort_query_component(
            model, 'child', 'parent')

    def get_parentsort_query_component(self, model):
        return self.get_relatedsort_query_component(
            model, 'parent', 'child')

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
