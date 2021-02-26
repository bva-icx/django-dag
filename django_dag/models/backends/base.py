from django.db import models, connection
from django.core.exceptions import ValidationError
from django.db.models import Case, When
from django_dag.exceptions import NodeNotReachableException

from deprecation import deprecated

def filter_order(queryset, field_names, values):
    """
    QuerySet filter
    Filter queryset where field_name in values, order results in
    the same order as values
    """
    if not isinstance(field_names, list):
        field_names = [field_names]
    case = []
    for pos, value in enumerate(values):
        when_condition = {field_names[0]: value, 'then': pos}
        case.append(When(**when_condition))
    order_by = Case(*case)
    filter_condition = {field_name + '__in': values
                        for field_name in field_names}
    return queryset.filter(**filter_condition).order_by(order_by)


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

        if child.pk in parent.ancestor_pks():
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

    def filter_order_ids(self, pk_list, respect_manager=True):
        if respect_manager:
            return filter_order(self._meta.default_manager, 'pk', pk_list)
        return filter_order(self._meta.base_manager, 'pk', pk_list)

    @property
    def ancestors(self):
        return self.filter_order_ids(self.ancestor_pks())

    @property
    def descendants(self):
        return self.filter_order_ids(self.descendant_pks())

    @property
    def clan(self):
        return self.filter_order_ids(self.clan_pks())

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

    def clan_pks(self):
        """
        Get a list of all the pks in the clan

        :rtype: list[ int, ... ]
        :return: list of pk
        """
        return self.ancestor_pks() + [self.pk, ] + self.descendant_pks()

    def descendant_pks(self):
        """
        Get a list of the node pk which are descendant

        :rtype: list[ int, ... ]
        :return: list of pk
        """
        raise NotImplementedError()

    def ancestor_pks(self):
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

        :rtype: QuerySet<<Node>>
        :return: The querySet of the root nodes
        """
        raise NotImplementedError()

    def get_leaves(self):
        """
        Find the leaf nodes in the dag attached to the currect node

        :rtype: QuerySet<<Node>>
        :return: The querySet of the leaf nodes
        """
        raise NotImplementedError()

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
