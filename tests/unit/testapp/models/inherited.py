
from django.db.models import CharField, Model
from django_dag.models import node_factory, edge_factory



###############################################################################
# Dag Nodes which are derived from a single concrete base


class ConcreteBaseNode(Model):
    parent_name = CharField(max_length=32, blank=True, null=True)


class InheritedConcreteNode(
    node_factory(
        'InheritedConcreteEdge',
        base_model=ConcreteBaseNode,
    )):
    """
    Simple Test Node with name field
    """
    name = CharField(max_length=32)

    def __str__(self):
        return '# %s' % self.name

    class Meta:
        app_label = 'testapp'


class InheritedConcreteEdge(edge_factory(InheritedConcreteNode, concrete=False)):
    """
    Simple Test Edge with name field
    """
    name = CharField(max_length=32, blank=True, null=True)

    class Meta:
        app_label = 'testapp'


###############################################################################
# Multiple Dag Nodes classes attached in a single DAG


class BaseDerivedNode(
    node_factory(
        'DerivedEdge',
    )):
    """
    Simple Test Node with name field
    """
    name = CharField(max_length=32)

    def __str__(self):
        return '# %s' % self.name

    class Meta:
        app_label = 'testapp'


class DerivedNodeA(BaseDerivedNode):
    child_name = CharField(max_length=32)

    class Meta:
        app_label = 'testapp'


class DerivedNodeB(BaseDerivedNode):
    child_name = CharField(max_length=32)

    class Meta:
        app_label = 'testapp'


class DerivedEdge(edge_factory(BaseDerivedNode, concrete=False)):
    """
    Simple Test Edge with name field
    """
    name = CharField(max_length=32, blank=True, null=True)

    class Meta:
        app_label = 'testapp'
