from ..models import backend
from enum import IntEnum, auto


class DagSortOrder(IntEnum):
    TOP_DOWN = auto()
    BREADTH_FIRST = auto()
    DEPTH_FIRST = auto()


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

    def with_sort_sequence(self, method, *args, **kwargs):
        """
        Provide a generalized sorted list of DAG nodes.

        This method call can be SLOW and if not using CTE's as this reqires a full table fetch
        to computer the order

        :param method `DagSortOrder`: The sort method to use.
        :return: QuerySet
        """
        def get_or_raise(methodname):
            if not hasattr(self, methodname):
                raise ValueError('Unknown sort method')
            return getattr(self, methodname)(self, *args, **kwargs)

        if method == DagSortOrder.TOP_DOWN:
            return self.with_top_down(*args, **kwargs)
        elif method == DagSortOrder.BREADTH_FIRST:
            return self.with_breath_first(*args, **kwargs)
        elif method == DagSortOrder.DEPTH_FIRST:
            return self.with_depth_first(*args, **kwargs)
        raise ValueError('Unknown sort method')


class BaseEdgeQuerySet(backend.ProtoEdgeQuerySet):
    pass
