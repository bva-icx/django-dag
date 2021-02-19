from .base import BaseNode
from django_dag.exceptions import NodeNotReachableException

class ProtoNode(BaseNode):
    ################################################################
    # Public API
    def descendant_pks(self):
            return list(self._descendant_pks())

    def _descendant_pks(self, cached_results=None):
        if cached_results is None:
            cached_results = dict()
        if self.pk in cached_results.keys():
            return cached_results[self.pk]
        else:
            res = set()
            for f in self.children.all():
                res.add(f.pk)
                res.update(f._descendant_pks(cached_results=cached_results))
            cached_results[self.pk] = res
            return res

    def ancestor_pks(self):
        return list(self._ancestor_pks())

    def _ancestor_pks(self, cached_results=None):
        if cached_results is None:
            cached_results = dict()
        if self in cached_results.keys():
            return cached_results[self.pk]
        else:
            res = set()
            for f in self.parents.all():
                res.add(f.pk)
                res.update(f._ancestor_pks(cached_results=cached_results))
            cached_results[self.pk] = res
            return res

    def path(self, target):
        if self == target:
            return []
        if target in self.children.all():
            return [target]
        if target in self.descendants:
            path = None
            for d in self.children.all():
                try:
                    desc_path = d.path(target)
                    if not path or len(desc_path) < len(path):
                        path = [d] + desc_path
                except NodeNotReachableException:
                    pass
        else:
            raise NodeNotReachableException()
        return path

    def get_roots(self):
        """
        Returns roots nodes, if any
        """
        at =  self.get_ancestors_tree()
        roots = set()
        for a in at:
            roots.update(a._get_roots(at[a]))
        return roots

    def _get_roots(self, at):
        """
        Works on objects: no queries
        """
        if not at:
          return set([self])
        roots = set()
        for a2 in at:
            roots.update(a2._get_roots(at[a2]))
        return roots

    def get_leaves(self):
        """
        Returns leaves nodes, if any
        """
        dt =  self.get_descendants_tree()
        leaves = set()
        for d in dt:
            leaves.update(d._get_leaves(dt[d]))
        return leaves

    def _get_leaves(self, dt):
        """
        Works on objects: no queries
        """
        if not dt:
          return set([self])
        leaves = set()
        for d2 in dt:
            leaves.update(d2._get_leaves(dt[d2]))
        return leaves


    def get_descendants_tree(self):
        """
        Returns a tree-like structure with progeny
        """
        tree = {}
        for f in self.children.all():
            tree[f] = f.get_descendants_tree()
        return tree

    def get_ancestors_tree(self):
        """
        Returns a tree-like structure with ancestors
        """
        tree = {}
        for f in self.parents.all():
            tree[f] = f.get_ancestors_tree()
        return tree
