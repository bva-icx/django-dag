
from django.db.models import CharField, Model
from django_dag.models import node_factory, edge_factory

################################################################
# Almost the simplist of DAG models

class BasicEdge(edge_factory('BasicNode', concrete=False)):
    """
    Simple Test Edge with name field
    """
    name = CharField(max_length=32, blank=True, null=True)

    class Meta:
        app_label = 'testapp'

class BasicNode(node_factory(BasicEdge)):
    """
    Simple Test Node with name field
    """
    name = CharField(max_length=32)

    def __str__(self):
        return '# %s' % self.name

    class Meta:
        app_label = 'testapp'


################################################################
# A Dag which return the created Edge on adding nodes
class BasicNodeES(node_factory('BasicEdgeES')):
    """
    Simple Test node for Edge Save Support
    """
    name = CharField(max_length=32)

    def __str__(self):
        return '# %s' % self.name

    class Meta:
        app_label = 'testapp'

class BasicEdgeES(edge_factory('BasicNodeES', concrete=False)):
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
