from enum import Enum, auto

from django.db import models
from django.db.models import OuterRef, Subquery, F, Min
from django.db import transaction
from django.core.exceptions import ObjectDoesNotExist
from django_dag import exceptions

class Position(Enum):
    FIRST = auto()
    LAST = auto()
    BEFORE = auto()
    AFTER = auto()

class BaseDagOrderController():
    """
    Interface class to provide support for edge or node ordering.
    """
    def __init__(self, sequence_field_name='sequence'):
        self.sequence_field_name = sequence_field_name

    ####################################################################
    # Functions needed for construction
    def get_node_sequence_field(self, ):
        """
        Returns a single field to be used to support ordering.
        Should the controller require more fields this can be a primary key
        to another model
        """
        raise NotImplementedError

    def get_edge_sequence_field(self, ):
        """
        Returns a single field to be used to support ordering.
        Should the controller require more fields this can be a primary key
        to another model
        """
        raise NotImplementedError

    ####################################################################
    # Sequence  / Ordering value support
    # These act on nodes
    # Note: These could become static members, as self dosen't seam to be
    #       used

    def key_between(self, instance, other, parent):
        """
        Return a key half way between this and other - assuming no other
        intermediate keys exist in the tree.
        """
        raise NotImplementedError

    def next_key(self, instance, parent):
        """
        Provide the next key in the sequence

        If the instance to not connected to parent this wil raise
        NodeNotReachableException

        :return: value to store in the sequence_field
        :raise: NodeNotReachableException

        """
        raise NotImplementedError

    def first_key(self):
        """
        Provide the initial /first key in the sequence

        Note: this is not the start of the key space, but the first key to be
        used
        """
        raise NotImplementedError

    def prev_key(self, instance, parent):
        """
        Provide the prev key in the sequence

        If the instance to not connected to parent this wil raise
        NodeNotReachableException

        :return: value to store in the sequence_field
        :raise: NodeNotReachableException
        """
        raise NotImplementedError

    ####################################################################
    # Queries related to a node
    def get_node_rel_sort_query_component(self, model, target, source):
        """
        Builds a query component that can be used for sorting a children of
        a dag Node.

        If the model is a instance of a DAG node the children should be
        so sorted in relation to the instance, if the model is a class then
        all result we be the indeterminate although in the reference implementation
        of edgeordering it is the first sequence. The precise details are
        implementation specific to the the concrete sequence_manager
        The returned query should be relative to the Node model

        :param model: A Node model or model instance
        :param target: the name of the field linking to the target
        :param source: the name of the field linking to the source node

        :return: A componet to be used in a further query
            F, Subquery etc. which will result in a value for the node sequence
        """
        raise NotImplementedError

    def get_edge_rel_sort_query_component(self, model, target, source):
        """
        Builds a query component that can be used for sorting a children of
        a dag Node.

        If the model is a instance of a DAG node the children should be
        so sorted in relation to the instance, if the model is a class then
        all result we be the indeterminate although in the reference implementation
        of edgeordering it is the first sequence. The precise details are
        implementation specific to the the concrete sequence_manager 
        The returned query should be relative to the NodeEdge model

        :param model: A Node model or model instance
        :param target: the name of the field linking to the target
        :param source: the name of the field linking to the source node

        :return: A componet to be used in a further query 
            F, Subquery etc. which will result in a value for the node sequence
        """
        raise NotImplementedError

    def get_sorted_edge_queryset(self, node, target, source):
        """
        :param node: A Node model instance
        :param target: the name of the field linking to the target
        :param source: the name of the field linking to the source node
        :returns: A queryset of the nodes
        """
        raise NotImplementedError

    ####################################################################
    # These need mapping onto the node object
    def get_first_child(self, basenode):
        node = self.get_sorted_edge_queryset(
            basenode, 'child', 'parent'
        ).first()
        return node.child if node else None

    def get_last_child(self, basenode):
        node = self.get_sorted_edge_queryset(
            basenode, 'child', 'parent'
        ).last()
        return node.child if node else None

    def get_first_parent(self, basenode):
        node = self.get_sorted_edge_queryset(
            basenode, 'parent', 'child'
        ).first()
        return node.parent if node else None

    def get_last_parent(self, basenode):
        node = self.get_sorted_edge_queryset(
            basenode, 'parent', 'child'
        ).last()
        return node.parent if node else None

    def get_first_sibling(self, basenode, parent_node):
        return parent_node.get_first_child()

    def get_last_sibling(self, basenode, parent_node):
        return parent_node.get_last_child()

    def get_next_sibling(self, basenode, parent_node):
        raise NotImplementedError

    def get_prev_sibling(self, basenode, parent_node):
        raise NotImplementedError

    def insert_child_before(self, descendant, parent_node, after, **kwargs):
        """
        Adds a node to the current node as a child directly before a sibling.

        :param descendant: The child node to add
        :param after: The child node to add or None
        :return: return result from edge link save
        """
        if after is None:
            sequence = self.first_key()
        else:
            before = after.get_prev_sibling(parent_node)
            if before:
                sequence =self.key_between(after, before, parent_node)
            else:
                sequence = self.prev_key(after, parent_node)

        kwargs.update({
            self.sequence_field_name: sequence
        })
        return parent_node.add_child( descendant, **kwargs)

    def insert_child_after(self, descendant, parent_node, before, **kwargs):
        """
        Adds a node to the current node as a child directly after a sibling.
    
        :param descendant: The child node to add
        :param after: The child node to add or None
        :return: return result from edge link save
        """
        if before is None:
            sequence = self.first_key()
        else:
            after = before.get_next_sibling(parent_node)
            if after:
                sequence =self.key_between(after, before, parent_node)
            else:
                sequence = self.next_key(before, parent_node)

        kwargs.update({
            self.sequence_field_name: sequence
        })
        return parent_node.add_child( descendant, **kwargs)

    def move_child_before(self, descendant, parent_node, before, **kwargs):
        """
        Move the edge link sequence so the child come before node

        Note: This does not change the parent or child just modify the
        relative position of the child to its siblings from this parent.

        :param descendant: The child node to add
        :param before: The child node to add
        :return: return result from edge link save
        :raises: InvalidNodeMove
        """
        if parent_node.children.filter(id__in = [before.pk, descendant.pk]).count() != 2:
            raise exceptions.InvalidNodeMove()

    def move_child_after(self, descendant, parent_node, after, **kwargs):
        """
        Move the edge link sequence so the child come before node

        Note: This does not change the parent or child just modify the
        relative position of the child to its siblings from this parent.

        :param descendant: The child node to add
        :param after: The child node to add
        :return: return result from edge link save
        :raises: InvalidNodeMove
        """
        # Check both are siblings of parent
        if parent_node.children.filter(id__in = [after.pk, descendant.pk]).count() != 2:
            raise exceptions.InvalidNodeMove()

    def insert_child(self, descendant, parent_node, position, **kwargs):
        """
        Adds a node to the current node as a child directly after a sibling.

        :param descendant: The child node to add
        :return: return result from edge link save
        """
        if position == Position.FIRST:
            insert_before = parent_node.get_first_child()
            return self.insert_child_before(descendant, parent_node, insert_before)
        elif position == Position.Last:
            insert_after = parent_node.get_last_child()
            return self.insert_child_before(descendant, parent_node, insert_after)
        else:
            raise exceptions.InvalidNodeInsert()

    def move_node(self, descendant, origin_parent, destination_parent,
            destination_sibling, position, **kwargs):
        """
        Generic node restructure. Moves a Node preserving the edge object.

        This allows a node to be moved whilst the edge link any any supplimentry
        data on the Edge is preserved,
        If there is no destination sibling then the node is added as a child
        using the default sequence position

        :param descendant: The node to move
        :param origin_parent: The nodes current parent
        :param destination_parent: The node final parent
        :param destination_sibling: The node final sibing or None
        :param position: `class:Position` or None
        """
        sequence = None
        if destination_sibling is None:
            sequence = self.first_key()
        else:
            other_sibling = None
            if position == Position.BEFORE:
                other_sibling = self.get_prev_sibling(
                    destination_sibling, destination_parent)
                if other_sibling:
                    sequence = self.key_between(other_sibling, destination_sibling,
                        destination_parent)
            if position == Position.FIRST or (
                position == Position.BEFORE and not sequence
            ):
                other_sibling = self.get_first_sibling(
                    destination_sibling, destination_parent)
                if other_sibling:
                    sequence = self.prev_key(other_sibling, destination_parent)

            if position == Position.AFTER:
                other_sibling = self.get_next_sibling(
                    destination_sibling, destination_parent)
                if other_sibling:
                    sequence = self.key_between(destination_sibling, other_sibling,
                        destination_parent)
            if position == Position.LAST or (
                position is None ) or (
                position == Position.AFTER and not sequence
            ):
                other_sibling = self.get_last_sibling(
                    destination_sibling, destination_parent)
                if other_sibling:
                    sequence = self.next_key(other_sibling, destination_parent)

        assert sequence is not None
        kwargs.update({ self.sequence_field_name: sequence })
        return self._move_node(descendant, origin_parent, destination_parent,
            **kwargs)


class BaseDagNodeOrderController(BaseDagOrderController):
    @classmethod
    def get_edge_sequence_field(cls, ):
        """
        Returns a single field to be used to support ordering.
        Should the controller require more fields this can be a primary key
        to another model
        """
        return None

    def get_node_rel_sort_query_component(self, model, targetname, sourcename):
        """
        Builds a query component that can be used for sorting a children of
        a dag Node.
        The returned query is relative to the Node

        :return: django F() expressions
        """
        return F(self.sequence_field_name)

    def get_edge_rel_sort_query_component(self, model, targetname, sourcename):
        """
        Builds a query component that can be used for sorting a children of
        a dag Node.
        The returned query is relative to the Edge

        :param model: A Node model or model instance
        :return: Subquery etc. which will result in a value for the node sequence
        """
        return F("{}__{}".format(targetname,self.sequence_field_name))

    def get_sorted_edge_queryset(self, node, target, source):
        edge_model = node.get_edge_model()
        return edge_model.objects.filter(**{source: node}) \
            .select_related(target) \
            .order_by(
                "{}__{}".format(target, self.sequence_field_name)
            )

    def get_next_sibling(self, basenode, parent_node):
        edge_model = basenode.get_edge_model()
        sibling_node_edge = edge_model.objects.filter(
            parent=parent_node,
        ).filter(
            child__sequence__gt = basenode.sequence
        ).order_by(
            "child__%s"%(self.sequence_field_name,)
        ).select_related('child').first()
        return sibling_node_edge.child if sibling_node_edge else None

    def get_prev_sibling(self, basenode, parent_node):
        edge_model = basenode.get_edge_model()
        sibling_node_edge = edge_model.objects.filter(
            parent=parent_node,
        ).filter(
            child__sequence__lt = basenode.sequence
        ).order_by(
            "-child__%s"%(self.sequence_field_name,)
        ).select_related('child').first()
        return sibling_node_edge.child if sibling_node_edge else None

    def _move_node(self, descendant, origin_parent, destination_parent, **kwargs):
        edge = origin_parent.get_edge_model().objects \
            .filter(
                parent_id = origin_parent.pk,
                child_id = self.pk
            ).first()
        with transaction.atomic():
            setattr(descendant, self.sequence_field_name, kwargs.pop(self.sequence_field_name))
            edge.parent_id = destination_parent.pk
            node.save()
            return edge.save()


class BaseDagEdgeOrderController(BaseDagOrderController):
    @classmethod
    def get_node_sequence_field(cls, ):
        """
        Returns a single field to be used to support ordering.
        Should the controller require more fields this can be a primary key
        to another model
        """
        return None

    def get_node_rel_sort_query_component(self, model, targetname, sourcename):
        """
        Builds a query component that can be used for sorting a children of
        a dag Node.
        The returned query is relative  to the Node

        :param model: A Node model or model instance
        :return: Subquery etc. which will result in a value for the node sequence
        """
        sort_field_name = '_min_{}'.format(self.sequence_field_name)

        sequence = model.get_edge_model().objects
        if isinstance(model, models.Model):
            sequence = sequence.filter(**{sourcename:model})
        sequence = sequence.filter(
            **{targetname:OuterRef('pk')}
        ).annotate(
            **{sort_field_name: Min(self.sequence_field_name)}
        )
        # WARNING: this is non deterministic as we only use the first!
        return Subquery(sequence.values(sort_field_name)[:1])

    def get_edge_rel_sort_query_component(self, model, targetname, sourcename):
        """
        Builds a query component that can be used for sorting a children of
        a dag Node.
        The returned query is relative to the Edge

        :return: django F() expressions
        """
        return F(self.sequence_field_name)

    def get_sorted_edge_queryset(self, node, target, source):
        edge_model = node.get_edge_model()
        return edge_model.objects.filter(**{
                source: node
            }).select_related(target).order_by(self.sequence_field_name)

    def get_next_sibling(self, basenode, parent_node):
        edge_model = basenode.get_edge_model()
        try:
            sibling_node_edge = edge_model.objects.filter(
                parent=parent_node,
                sequence__gt = edge_model.objects.get(
                        parent=parent_node,
                        child=basenode
                    ).sequence
            ).order_by(self.sequence_field_name).select_related('child').first()
        except ObjectDoesNotExist:
            return None
        return sibling_node_edge.child if sibling_node_edge else None

    def get_prev_sibling(self, basenode, parent_node):
        edge_model = basenode.get_edge_model()
        try:
            sibling_node_edge = edge_model.objects.filter(
                parent=parent_node,
                sequence__lt = edge_model.objects.get(
                        parent=parent_node,
                        child=basenode
                    ).sequence
            ).order_by('-%s'%(self.sequence_field_name)).select_related('child').first()
        except ObjectDoesNotExist:
            return None
        return sibling_node_edge.child if sibling_node_edge else None

    def _move_node(self, descendant, origin_parent, destination_parent, **kwargs):
        edge = origin_parent.get_edge_model().objects \
            .filter(
                parent_id = origin_parent.pk,
                child_id = descendant.pk
            ).first()
        with transaction.atomic():
            setattr(edge, self.sequence_field_name, kwargs.pop(self.sequence_field_name))
            edge.parent_id = destination_parent.pk
            return edge.save()
