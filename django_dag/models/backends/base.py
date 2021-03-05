from django.db import models, connection
from django.core.exceptions import ValidationError
from django.db.models import Case, When
from django_dag.exceptions import NodeNotReachableException

from deprecation import deprecated

class BaseNode(object):
    """
    Main node abstract model
    """

    def __str__(self):
        return "# %s" % self.pk

    @staticmethod
    def circular_checker(parent, child):
        """
        Checks that the object is not an ancestor, avoid self links

        :param parent:
        :param child:
        :return: 
        """
        if parent == child:
            raise ValidationError('Self links are not allowed.')

        if child.pk in parent.get_ancestor_pks():
            raise ValidationError('The object is an ancestor.')

    @classmethod
    def get_node_model(cls, linkname='children'):
        """
        Get the node mode class used for this relation link.

        This is needed to ensure we are not using the base node class used
        during construction of the model. For polymorphic models the
        node's node._meta.model may be a subclass of this.

        :param linkname: The name of the field that links the nodes
        """
        return getattr(cls._meta.model, linkname).rel.model

    @classmethod
    def get_edge_model(cls, linkname='children'):
        """
        Get the edge model class used for this relation link

        :param linkname: The name of the field that links the nodes
        """
        return getattr(cls._meta.model, linkname).rel.through

    @property
    def is_root(self):
        """
        Check if has children and not ancestors

        :rtype: boolean
        :return: True is the node is at the top of the DAG
        """
        return bool(self.children.exists() and not self.parents.exists())

    @property
    def is_leaf(self):
        """
        Check if has ancestors and not children

        :rtype: boolean
        :return: True is the node is at the bottom of the DAG
        """
        return bool(self.parents.exists() and not self.children.exists())

    @property
    def is_island(self):
        """
        Check if has no ancestors nor children

        :rtype: boolean
        :return: True is the node has neither ancestors or children
        """
        return bool(not self.children.exists() and not self.parents.exists())

    def add_child(self, descendant, **kwargs):
        """
        Adds a node to the current node as a child

        :param descendant: The child node to add
        :return: return result from edge link save
        """
        kwargs.update({'parent' : self, 'child' : descendant })
        disable_check = kwargs.pop('disable_circular_check', False)
        cls = self.children.through(**kwargs)
        return cls.save(disable_circular_check=disable_check)

    def add_parent(self, parent, *args, **kwargs):
        """
        Adds a node to the current node it parent

        :param parent: The parent node to add
        :return: return result from edge link save
        """
        return parent.add_child(self, **kwargs)

    def remove_child(self, descendant):
        """
        Detach a child node from this 'parent' node.

       :param descendant: The child node to detach
        """
        self.children.through.objects.get(parent = self, child = descendant).delete()

    def remove_parent(self, parent):
        """
        Detach a parent node from this 'child' node.

       :param parent: The parent node to detach
        """
        parent.children.through.objects.get(parent = parent, child = self).delete()

    def distance(self, target):
        """
        Finds the shortest hops count to the target vertex
        :raises: NodeNotReachableException
        :rtype: int
        :return: The shortest hops count to the target vertex
        """
        try:
            return len(self.get_paths(target, downwards=True)[0])
        except NodeNotReachableException as err:
            pass
        return - len(self.get_paths(target, downwards=False)[0])

    @property
    def clan(self):
        return self.ancestors + [self, ] + self.descendants

    @property
    def ancestors(self):
        raise NotImplementedError()

    @property
    def descendants(self):
        raise NotImplementedError()

    def get_clan_pks(self):
        """
        Get a list of all the pks in the clan

        :rtype: list[ int, ... ]
        :return: list of pk
        """
        return self.get_ancestor_pks() + [self.pk, ] + self.get_descendant_pks()

    def get_descendant_pks(self):
        """
        Get a list of the node pk which are descendant

        :rtype: list[ int, ... ]
        :return: list of pk
        """
        raise NotImplementedError()

    def get_ancestor_pks(self):
        """
        Get a list of the node pk which are ancestor

        :rtype: list[ int, ... ]
        :return: list of pk
        """
        raise NotImplementedError()

    def get_paths(self, target, use_edges=False, downwards=None):
        """
        Finds a list of the shortest paths between two nodes.

        Each of the found paths are:
            A list of nodes corresponding to the sequence needed to move from
            the source node (self) to the target node.

            Each step in the sequence can be represented by:
                * the target node [default]
                * the edge [if use_edges set True]

            If the source node and the target are the same node then an empty path is
            returned.
            If the source and target node are not connected then the
            NodeNotReachableException exception is raised

        If the target is the first in the list then it is an ancestor of the
        source else if it is the last it is a descendant of the source node.
        The order of the list of paths is undefined.

        :raises: NodeNotReachableException
        :params use_edges: Controls the return object
             If True: return the edges joining nodes.
             If False: (default) return the target node
        :params downwards: Controls the return object
            If None: (default) Bi-Directional search.
            If True: Down the tree, parent to child.
            If False: up the tree child to parent


        :rtype: list<QuerySet<Node>>
        :return:  List of query sets for each
        """
        raise NotImplementedError()

    def get_roots(self):
        """
        Find the root nodes in the dag attached to the currect node

        If the dag is node is an island this will return a query set
        containing just the source node (self).

        :rtype: QuerySet<<Node>>
        :return: The querySet of the root nodes
        """
        raise NotImplementedError()

    def get_leaves(self):
        """
        Find the leaf nodes in the dag attached to the currect node

        If the dag is node is an island this will return a query set
        containing just the source node (self).

        :rtype: QuerySet<<Node>>
        :return: The querySet of the leaf nodes
        """
        raise NotImplementedError()

    ################################################################
    # functions that are redirected to the sequence_manager
    def get_first_child(self):
        return self.sequence_manager.get_first_child(self)

    def get_last_child(self):
        return self.sequence_manager.get_last_child(self)

    def get_first_parent(self):
        return self.sequence_manager.get_first_parent(self)

    def get_last_parent(self):
        return self.sequence_manager.get_last_parent(self)

    def get_next_sibling(self, parent_node):
        return self.sequence_manager.get_next_sibling(self, parent_node)

    def get_prev_sibling(self, parent_node):
        return self.sequence_manager.get_prev_sibling(self, parent_node)

    def insert_child_before(self, descendant, before, **kwargs):
        """
        Adds a node to the current node as a child directly before a sibling.

        :param descendant: The child node to add
        :param before: The child node to add
        :return: return result from edge link save
        """
        return self.sequence_manager.insert_child_before(
            descendant, self, before, **kwargs)

    def insert_child_after(self, descendant, after, **kwargs):
        """
        Adds a node to the current node as a child directly after a sibling.

        :param descendant: The child node to add
        :param after: The child node to add
        :return: return result from edge link save
        """
        return self.sequence_manager.insert_child_after(
            descendant, self, after, **kwargs)

    ################################################################
    # Legacy functions
    @deprecated()
    def node_set(self):
        return self.clan

    @deprecated()
    def descendants_set(self):
        return self.descendants

    @deprecated()
    def node_set(self):
        return self.clan

    @deprecated()
    def descendants_set(self):
        return self.descendants

    @deprecated()
    def ancestors_set(self):
        return self.ancestors

    @deprecated()
    def descendants_tree(self):
        return self.get_descendants_tree()

    @deprecated()
    def ancestors_tree(self):
        return self.get_ancestors_tree()

    @deprecated()
    def path(self, target):
        """
        The first found path between two nodes that is the shortest
        (see get_nodes())
        :raises: NodeNotReachableException
        :rtype: QuerySet<Node>
        :return:  List of query sets for each
        """
        return self.get_paths(target,
                use_edges=False, downwards=True)[0]
