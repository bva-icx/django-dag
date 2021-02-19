"""
A class to model hierarchies of objects following
Directed Acyclic Graph structure.

Some ideas stolen from:
    from https://github.com/stdbrouw/django-treebeard-dag
"""
from django.db import models

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

        def __str__(self):
            return "%s is child of %s" % (self.child, self.parent)

        def save(self, *args, **kwargs):
            if not kwargs.pop('disable_circular_check', False):
                self.parent.get_node_model(self.parent).circular_checker(
                    self.parent, self.child)
            super(Edge, self).save(*args, **kwargs) # Call the "real" save() method.

    return Edge

def node_factory( edge_model,
        children_null = True,
        base_model = models.Model,
        field = models.ManyToManyField,
        ordering = False,
    ):
    """Dag Node factory"""

    from .backends.standard import ProtoNode
    class Node(base_model, ProtoNode):
        class Meta:
            abstract = True

        children  = models.ManyToManyField(
                'self',
                blank = children_null,
                symmetrical = False,
                through = edge_model,
                related_name = 'parents')
    return Node
