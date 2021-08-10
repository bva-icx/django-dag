from django.core.exceptions import ValidationError
from django_dag.exceptions import (
    NodeNotReachableException,
    InvalidNodeMove,
)

from deprecated.sphinx import deprecated


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
        :return: True if there is not error
        :raise: ValidationError
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
        Check if node has no ancestors

        :rtype: boolean
        :return: True is the node is at the top of the DAG
        """
        return not self.parents.exists()

    @property
    def is_leaf(self):
        """
        Check if the node has not children

        :rtype: boolean
        :return: True is the node is at the bottom of the DAG
        """
        return not self.children.exists()

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
        kwargs.update({'parent': self, 'child': descendant})
        disable_check = kwargs.pop('disable_circular_check', False)

        if self.sequence_manager and self.sequence_manager.get_node_sequence_field():
            sequencename = self.sequence_manager.sequence_field_name
            sequence = kwargs.pop(sequencename, None)
            if sequence:
                setattr(descendant, sequencename, sequence)
                descendant.save()
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
        self.children.through.objects.get(
            parent=self, child=descendant).delete()

    def remove_parent(self, parent):
        """
        Detach a parent node from this 'child' node.

       :param parent: The parent node to detach
        """
        parent.children.through.objects.get(parent=parent, child=self).delete()

    def distance(self, target, directed=True):
        """
        Finds the shortest hops count to the target vertex

        By default this returns the SIGNED distances with
        negative results indicating direction for child to
        parent (eg against the edges direction)

        :param target: destination node
        :param directed: If true return a signed distances, with
                         a negative value for distances of child to parent.
        :raises: NodeNotReachableException
        :rtype: int
        :return: The shortest hops count to the target vertex
        """
        reversing_sign = -1 if directed else 1
        try:
            return len(self.get_paths(target, downwards=True)[0])
        except NodeNotReachableException as err:  # noqa: F841
            pass
        return reversing_sign * len(self.get_paths(target, downwards=False)[0])

    @property
    def clan(self):
        return self.ancestors | self.get_node_model().objects.filter(pk=self.pk) | self.descendants

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

    def insert_child(self, descendant, position, **kwargs):
        """
        Adds a node to the current node as a child directly in the psotion
        specified.

        :param position: `class:Position`
        """
        return self.sequence_manager.insert_child(
            descendant, self, position, **kwargs)

    def move_child_before(self, descendant, before, **kwargs):
        """
        Move the edge link sequence so the child come before node

        Note: This does not change the parent or child just modify the
        relative position of the child to its siblings from this parent.

        :param descendant: The child node to add
        :param before: The child node to add
        :return: return result from edge link save
        :raises: InvalidNodeMove
        """
        return self.sequence_manager.move_child_before(
            descendant, self, before, **kwargs)

    def move_child_after(self, descendant, after, **kwargs):
        """
        Move the edge link sequence so the child come before node

        Note: This does not change the parent or child just modify the
        relative position of the child to its siblings from this parent.

        :param descendant: The child node to add
        :param after: The child node to add
        :return: return result from edge link save
        :raises: InvalidNodeMove
        """
        return self.sequence_manager.move_child_after(
            descendant, self, after, **kwargs)

    def move_node(self, origin_parent, destination_parent,
            destination_sibling=None, position=None):
        """
        Generic node restructure. Moves this Node preserving the edge object.

        This allows a node to be moved whilst the edge link any any supplimentry
        data on the Edge is preserved,
        If there is no destination sibling then the node is added as a child
        using the default sequence position

        :NOTE The params destination_sibling, position are only supported on
            ordered dags

        :param origin_parent: The nodes current parent
        :param destination_parent: The node final parent
        :param destination_sibling: The node final sibing or None
        :param position: `class:Position` or None
        """
        edge = None
        if origin_parent:
            edge = origin_parent.get_edge_model().objects \
                .filter(
                    parent_id=origin_parent.pk,
                    child_id=self.pk
            ).first()
        elif destination_parent and position:
            return destination_parent.insert_child(
                self, position
            )

        if edge is None:
            raise InvalidNodeMove()
        destination_parent.circular_checker(destination_parent, self)

        if self.sequence_manager:
            return self.sequence_manager.move_node(
                self, origin_parent, destination_parent,
                destination_sibling=destination_sibling,
                position=position,
            )

        edge.parent_id = destination_parent.pk
        return edge.save()

    ################################################################
    # Legacy functions
    @deprecated(version='2.0', reason="Replaced by descendants")
    def descendants_set(self):
        return self.descendants

    @deprecated(version='2.0', reason="Replaced by clan")
    def node_set(self):
        return self.clan

    @deprecated(version='2.0', reason="Replaced by ancestors")
    def ancestors_set(self):
        return self.ancestors

    @deprecated(version='2.0')
    def descendants_tree(self):
        return self.get_descendants_tree()

    @deprecated(version='2.0')
    def ancestors_tree(self):
        return self.get_ancestors_tree()

    @deprecated(version='2.0', reason="Replaced by paths as multiple paths are possible")
    def path(self, target):
        """
        The first found path between two nodes that is the shortest
        (see get_nodes())
        :raises: NodeNotReachableException
        :rtype: QuerySet<Node>
        :return:  List of query sets for each
        """
        return self.get_paths(target, use_edges=False, downwards=True)[0]
