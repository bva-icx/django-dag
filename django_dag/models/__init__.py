"""
A class to model hierarchies of objects following
Directed Acyclic Graph structure.

Some ideas stolen from:
    from https://github.com/stdbrouw/django-treebeard-dag
"""
from django.db import models
from django.conf import settings
from importlib import import_module
from .order_control import BaseDagOrderController
from .backends.base import BaseNode as NodeBase

module_name = getattr(settings, 'DJANGO_DAG_BACKEND', "django_dag.models.backends.standard")
backend = import_module(module_name)

BaseNodeManager = backend.ProtoNodeManager
BaseEdgeManager = backend.ProtoEdgeManager
BaseNodeQuerySet = backend.ProtoNodeQuerySet
BaseEdgeQuerySet = backend.ProtoEdgeQuerySet

__all__ =[
    "edge_manager_factory",
    "node_manager_factory",
    "edge_factory",
    "node_factory",
    "BaseNodeManager",
    "BaseEdgeManager",
    "BaseNodeQuerySet",
    "BaseEdgeQuerySet",
    "BaseDagOrderController",
]


def _get_base_manager(base_model, base_merge_manager):
    _default_manager_class = None
    _default_manager = getattr(base_model, '_default_manager', None)
    if _default_manager:
        _default_manager_class = base_model._default_manager.__class__
    if _default_manager_class:
        class MergerManager(_default_manager_class, base_merge_manager):
            pass
        _default_manager_class = MergerManager
    else:
        _default_manager_class = base_merge_manager
    return _default_manager_class

def edge_manager_factory(base_manager_class, ordering=None):
    class EdgeManager(base_manager_class):
        pass

    EdgeManager.ordering = ordering
    return EdgeManager


def edge_factory( node_model,
        child_to_field = "id",
        parent_to_field = "id",
        ordering = False,
        concrete = True,
        base_model = models.Model,
        manager = None,
        queryset = None,
    ):
    """
    Dag Edge factory
    """
    if isinstance(node_model, str):
        try:
            node_model_name = node_model.split('.')[-1]
        except IndexError:
            node_model_name = node_model
    else:
        node_model_name = node_model._meta.model_name

    edge_manager = edge_manager_factory(
            _get_base_manager(base_model, backend.ProtoNodeManager),
            ordering,
        ) if manager is None else manager

    class Edge(base_model):
        class Meta:
            abstract = not concrete

        objects = edge_manager() if queryset is None else edge_manager.from_queryset(queryset)()

        parent = models.ForeignKey(
            node_model,
            related_name = "%s_child" % node_model_name,
            to_field = parent_to_field,
            on_delete=models.CASCADE
            )
        child = models.ForeignKey(
            node_model,
            related_name = "%s_parent" % node_model_name,
            to_field = child_to_field,
            on_delete=models.CASCADE
            )

        if isinstance(ordering, BaseDagOrderController):
            sequence_field_name = ordering.sequence_field_name
            locals()[sequence_field_name] = ordering.get_edge_sequence_field()
            sequence_manager = ordering
        else:
            sequence_manager = None

        def __str__(self):
            return "%s is child of %s" % (self.child, self.parent)

        def save(self, *args, **kwargs):
            if not kwargs.pop('disable_circular_check', False):
                self.parent.get_node_model().circular_checker(
                    self.parent, self.child)
            super(Edge, self).save(*args, **kwargs) # Call the "real" save() method.

    #BASE_EDGE_TYPES.append(Edge)
    return Edge


def node_manager_factory(base_manager_class, ordering=None, ):
    class NodeManager(base_manager_class):
        sequence_manager = ordering

        def ordered(self,):
            """
            If the Node ordering then this modifies the queryset to order
            the nodes by the Node or Edge sequence as defined by the model

            The resultant Node is annotated with the _sequence value

            :return: QuerySet,
            """
            if not self.sequence_manager:
                return self

            sequence_field_name = self.sequence_manager.sequence_field_name
            if not hasattr(self, 'target_field_name'):
                # If we don't haave a target_field_name we are probably not a related
                # manager so we can not order by
                if getattr(self.model, sequence_field_name , None) is None:
                    # FIXME: Decide how to manage assert below
                    assert False, "You cannot order if you don't know what to order by"
                    return self
                else:
                    # Node sequence order on node
                    target, source = None, None
            else:
                # Related field order
                target, source = self.target_field_name, self.source_field_name

            instance_or_model = getattr(self, 'instance', self.model)
            return self.get_queryset().annotate(
                _sequence=self.sequence_manager.get_relatedsort_query_component(
                    instance_or_model, target, source)
            ).order_by('_sequence')


        #def get_queryset(self):
        #    qs = super().get_queryset()
        #    if not self.sequence_manager:
        #        return qs
        #    return qs

    class NodeQuerySet(models.QuerySet):
        sequence_manager = ordering

    return NodeManager


def node_factory( edge_model,
        children_null = True,
        base_model = models.Model,
        field = models.ManyToManyField,
        ordering = False,
        manager = None,
        queryset = None,
    ):
    """Dag Node factory"""

    node_manager = node_manager_factory(
            _get_base_manager(base_model, backend.ProtoNodeManager),
            ordering,
        ) if manager is None else manager

    class Node(base_model, backend.ProtoNode):
        class Meta:
            abstract = True

        objects = node_manager() if queryset is None else node_manager.from_queryset(queryset)()
        children  = models.ManyToManyField(
                'self',
                blank = children_null,
                symmetrical = False,
                through = edge_model,
                related_name = 'parents')

        if isinstance(ordering, BaseDagOrderController):
            sequence_field_name = ordering.sequence_field_name
            locals()[sequence_field_name] = ordering.get_node_sequence_field()
            sequence_manager = ordering
        else:
            sequence_manager = None


    return Node
