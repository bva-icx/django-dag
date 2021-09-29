from enum import IntEnum, auto

from ..models import backend


class DagSortOrder(IntEnum):
    DEFAULT = auto()
    NODE_PK = auto()
    NODE_SEQUENCE = auto()


class BaseNodeQuerySet(backend.ProtoNodeQuerySet):
    def roots(self, node=None):
        """
        Limits the current queryset those which are root nodes.

        Returns a queryset of the Root nodes (nodes with no parents) in the
        Node model which are part of the current query.

        :param node: If a node instance is specified, further limits to only
            the roots ancestors for that node.
        :return: QuerySet
        """
        if node is not None:
            return (self & node.get_roots()).distinct()
        return self.filter(parents__isnull=True)

    def leaves(self, node=None):
        """
        Limits the current queryset those which are root nodes.

        Returns a queryset of the Root nodes (nodes with no children) in the
        Node model which are part of the current query.

        :param node: If a node instance is specified, further limits to only
            the leaves decendants for that node.
        :return: QuerySet
        """
        if node is not None:
            return (self & node.get_leaves()).distinct()
        return self.filter(children__isnull=True)

    def with_sort_sequence(self, method=DagSortOrder.DEFAULT, *args,
            **kwargs):
        """
        Provides annotations to enable sorting of the DAG nodes.

        Notes:
            * This method call can be SLOW and if not using CTE's as this reqires a full
              dag fetch to computer the order.
            * Nodes with multiple roots will be present in the results multiple time, as
              the dag is 'unrolled'.

        This add the annotations:
            * dag_depth: Depth of the node from it root.
            * dag_node_path: A str representation list of the nodes visited from root to node


        Differing sorting orders can be achieved using these annotations.
            DFS: Depth First Search
                1: 'Inorder (Left, Root, Right)' sort
                    Unsupported
                2: 'preorder (Root, Left, Right)' sort
                    This sort order is available for both dag with and without
                    an implicit sequence, if you are using unsorted dags it is
                    assumed the 'pk' is the sequence.
                    ie:
                        nodesQuery.with_sort_sequence().orderby('dag_node_path')

                    or for sortable dags:
                        nodesQuery.with_sort_sequence().orderby('dag_sequence_path')

                    Sortable dags can be treated as unsorted and still use order by
                    'dag_pk_path'.
                3: 'Postorder (Left, Right, Root)' sort
                    Unsupported

        :param method `DagSortOrder`: The sort method to use.
            DEFAULT: For sortable dags this is DagSortOrder.NODE_SEQUENCE
                else DagSortOrder.NODE_PK
            NODE_PK : Add the basic annotations
            NODE_SEQUENCE : The basic annotations and 'dag_sequence_path'
        :return: QuerySet
        """
        if method == DagSortOrder.DEFAULT:
            method = DagSortOrder.NODE_SEQUENCE if (
                self.model.sequence_manager) else DagSortOrder.NODE_PK

        if method == DagSortOrder.NODE_PK:
            return self.with_pk_path(*args, **kwargs)
        elif method == DagSortOrder.NODE_SEQUENCE:
            return self.with_sequence_path(*args, **kwargs)

        raise ValueError('Unknown sort method')


class BaseEdgeQuerySet(backend.ProtoEdgeQuerySet):
    pass
