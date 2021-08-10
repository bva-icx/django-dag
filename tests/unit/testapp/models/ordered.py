from django.db.models import CharField, Model
from django_dag.models import node_factory, edge_factory

from .ordersort import DagEdgeIntSorter, DagNodeIntSorter


################################################################
# A Dag in which the children of a node are ordered in respect each
# other. eg the order is attached to the node edge


class OrderedEdge(edge_factory(
        'EdgeOrderedNode',
        concrete=False,
        ordering=DagEdgeIntSorter(),
    )):
    """
    Test Edge which support return the edge on save
    """
    name = CharField(max_length=32, blank=True, null=True)

    class Meta:
        app_label = 'testapp'

    def save(self, *args, **kwargs):
        """
        Save the edge link
        """
        # We are returning self here so that the django-dag functions
        # add_child and add_parent
        # return the edge used to link the parent to the child
        super().save(*args, **kwargs)
        return self


class EdgeOrderedNode(node_factory(
        OrderedEdge,
        ordering = DagEdgeIntSorter(),
    )):
    """
    Simple Test node for Edge Save Support
    """
    name = CharField(max_length=32)

    def __str__(self):
        return '# %s' % self.name

    class Meta:
        app_label = 'testapp'


################################################################
# A Dag in which all nodes have an implicit ordered.
# ie the order is part of the node no the edge

class OrderedNode(node_factory(
        'NodeOrderedEdge',
        ordering = DagNodeIntSorter(),
    )):
    """
    Simple Test node for Edge Save Support
    """
    name = CharField(max_length=32)

    def __str__(self):
        return '# %s' % self.name

    class Meta:
        app_label = 'testapp'

class NodeOrderedEdge(edge_factory(
        'OrderedNode',
        concrete=False,
        ordering=DagNodeIntSorter(),
    )):
    """
    Test Edge which support return the edge on save
    """
    name = CharField(max_length=32, blank=True, null=True)

    class Meta:
        app_label = 'testapp'

    def save(self, *args, **kwargs):
        """
        Save the edge link
        """
        # We are returning self here so that the django-dag functions
        # add_child and add_parent
        # return the edge used to link the parent to the child
        super().save(*args, **kwargs)
        return self
