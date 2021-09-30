from _typeshed import OpenBinaryModeReading


Django DAG
----------

.. Note:
    This is an unoffical 2 branch for Python 3 and Django2.2+ compatibility,

    This version number has been bumped purely to aid the installation tools,
    and may not have all the features that the upstream 2 ends up with.

    It is our intention to get as mush of this submitted as PR although
    there is significant changes and some breaking changes to the version 1
    branch

    This version adds support for:
    * Node Ordering
    * Node Sorting
    * Runtime Optional CTE support


Django-dag is a small reusable app which implements a Directed Acyclic Graph.

Usage
.....

Django-dag uses abstract base classes, to use it you must create your own
concrete classes that inherit from Django-dag classes.

The `dag_test` app contains a simple example and a unit test to show
you its usage.

Example::

    class ConcreteNode(node_factory('ConcreteEdge')):
        """
        Test node, adds just one field
        """
        name = models.CharField(max_length = 32)

    class ConcreteEdge(edge_factory(ConcreteNode, concrete = False)):
        """
        Test edge, adds just one field
        """
        name = models.CharField(max_length = 32, blank = True, null = True)


Tests
.....

Unit tests can be run with just django installed at the base directory by running
   `python manage.py test`


Breaking changes
................
   * The name of function Parents remove and parents is now a ManyRelatedManager the same
     as children.  Change parents() to parents.all()
   * node_set() is now the clan property  (depreciated version available)
   * descendant_set() is now the descendants property (depreciated version available)
   * ancestor_set() is now the ancestors property (depreciated version available)
   * is_leaf, is_island and is_root are all now properties
   * is_leaf is now true even if the node is an island
   * is_root is now true even if the node is an island
   * get_roots, get_leaves now return self id node is an island
