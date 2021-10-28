from enum import IntEnum, auto

from ..models import backend


class DagSortOrder(IntEnum):
    DEFAULT = auto()
    NODE_PK = auto()
    NODE_SEQUENCE = auto()


DEFAULT_PATH_PADDING_SIZE = 4
DEFAULT_PATH_PADDING_CHAR = '0'
DEFAULT_PATH_SEPERATOR = ','


class BaseNodeQuerySet(backend.ProtoNodeQuerySet):
    path_padding_size = DEFAULT_PATH_PADDING_SIZE
    path_padding_char = DEFAULT_PATH_PADDING_CHAR
    path_seperator = DEFAULT_PATH_SEPERATOR

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

        This add the annotations:
            * dag_depth: Depth of the node from it root.
            * dag_node_path: A str representation list of the nodes visited from root to node

        Differing sorting orders can be achieved using these annotations.
            DFS - Depth First Search
                1 - 'Inorder (Left, Root, Right)' sort
                    Unsupported

                2 - 'preorder (Root, Left, Right)' sort
                    This sort order is available for both dag with and without
                    an implicit sequence, if you are using unsorted dags it is
                    assumed the 'pk' is the sequence.

                    Example::

                        nodesQuery.with_sort_sequence().order_by('dag_node_path')

                    or for sortable dags:

                    Example::

                        nodesQuery.with_sort_sequence().order_by('dag_sequence_path')

                    Sortable dags can be treated as unsorted and still use order by
                    'dag_node_path' ordering.

                3 - 'Postorder (Left, Right, Root)' sort
                    This can be achieved, but isn't directly support as will require multiple
                    queries to obtain value needed for maxdepth and possibly padsize.
                    `maxdepth` value needs to be the maximum depth of the dag
                    other valuse needed:
                    `padsize` is the size to render the sequence or pk field
                    `sepchar` is the path seperator charater this can be ''
                    `padchar` is the a character > in sort order than the any char in the sequence of pk field.

                    Example::

                        from django.db.models import TextField
                        from django.db.models.expressions import F, Value
                        from django.db.models.functions import Cast, Concat, RPad
                        nodesQuery.with_sort_sequence().annotate(
                            dag_postorder_path=RPad(
                                Concat(
                                    Cast(F('dag_sequence_path'), output_field=TextField()),
                                    Value(sepchar),
                                ),
                                (padsize + 1) * maxdepth,
                                Value(padchar)
                            )
                        ).order_by('dag_postorder_path')

            BFT - Breath First Search
                This sort order available by
                Example::

                    nodesQuery.with_sort_sequence().order_by('dag_depth, 'dag_sequence_path')

        Notes:
            * This method call can be SLOW and if not using CTE's as this reqires a full
              dag fetch to computer the order.
            * Nodes with multiple roots will be present in the results multiple time, as
              the dag is 'unrolled'.


        :param method `DagSortOrder`: The sort method to use.
            DEFAULT:: For sortable dags this is DagSortOrder.NODE_SEQUENCE else DagSortOrder.NODE_PK
            NODE_PK:: Add the basic annotations 'dag_pk_path'
            NODE_SEQUENCE:: The basic annotations and 'dag_sequence_path'

        :param padsize int: Number of characters of each level of the dag path
        :param padchar str: Character used to pad the path with
        :param sepchar str: Character to separate
        :param name str: The resultant annnotations name which will contain the path

        :return: QuerySet
        """
        self._padding_size = kwargs.pop('padsize', self.path_padding_size)
        self._padding_char = kwargs.pop('padchar', self.path_padding_char)
        self._path_seperator = kwargs.pop('sepchar', self.path_seperator)

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
