"""
A class to model hierarchies of objects following
Directed Acyclic Graph structure.

Some ideas stolen from:
    from https://github.com/stdbrouw/django-treebeard-dag
"""
from django.db import models

from .order_control import BaseDagOrderController

def edge_factory( node_model,
        child_to_field = "id",
        parent_to_field = "id",
        ordering = False,
        concrete = True,
        base_model = models.Model,
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

    class Edge(base_model):
        class Meta:
            abstract = not concrete

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


def node_manager_factory(base_model, ordering=None):
    _default_manager = getattr(base_model, '_default_manager', models.Manager)
    if getattr(base_model, '_default_manager', None):
        _default_manager = base_model._default_manager.__class__

    class NodeManager(_default_manager):
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

        def get_first_child(self, ):
            return self.sequence_manager.get_first_child(self)
        def get_last_child(self):
            return self.sequence_manager.get_last_child(self)
        def get_first_parent(self, ):
            return self.sequence_manager.get_first_parent(self)
        def get_last_parent(self):
            return self.sequence_manager.get_last_parent(self)

        #def get_queryset(self):
        #    qs = super().get_queryset()
        #    if not self.sequence_manager:
        #        return qs
        #    return qs

    class NodeQuerySet(models.QuerySet):
        sequence_manager = ordering

    #return NodeManager, NodeQuerySet
    return NodeManager


def node_factory( edge_model,
        children_null = True,
        base_model = models.Model,
        field = models.ManyToManyField,
        ordering = False,
    ):
    """Dag Node factory"""

    from .backends.standard import ProtoNode
    #mgr, qset = node_manager_factory(base_model, ordering)

    class Node(base_model, ProtoNode):
        class Meta:
            abstract = True

        objects = node_manager_factory(base_model, ordering)()
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
