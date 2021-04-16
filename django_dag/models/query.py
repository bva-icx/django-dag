from ..models import backend

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


class BaseEdgeQuerySet(backend.ProtoEdgeQuerySet):
    pass
