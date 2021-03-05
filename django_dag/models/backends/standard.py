from django.db import models
from django_dag.exceptions import NodeNotReachableException
from .base import BaseNode


ProtoNodeManager = models.Manager
ProtoEdgeManager = models.Manager

class ProtoNode(BaseNode):
    ################################################################
    # Public API
    @property
    def descendants(self):
            return list(self._get_descendant())

    def get_descendant_pks(self):
            return list(self._get_descendant(node_to_cache_attr=lambda x:x.pk))

    def _get_descendant(self, cached_results=None, node_to_cache_attr=lambda x:x):
        if cached_results is None:
            cached_results = dict()
        if node_to_cache_attr(self) in cached_results.keys():
            return cached_results[node_to_cache_attr(self)]
        else:
            res = set()
            for f in self.children.all():
                res.add(node_to_cache_attr(f))
                res.update(f._get_descendant(
                    cached_results=cached_results,
                    node_to_cache_attr=node_to_cache_attr
                    ))
            cached_results[node_to_cache_attr(self)] = res
            return res

    @property
    def ancestors(self):
        return list(self._get_ancestor())

    def get_ancestor_pks(self):
        return list(self._get_ancestor(node_to_cache_attr=lambda x:x.pk))

    def _get_ancestor(self, cached_results=None, node_to_cache_attr=lambda x:x):
        if cached_results is None:
            cached_results = dict()
        if node_to_cache_attr(self) in cached_results.keys():
            return cached_results[node_to_cache_attr(self)]
        else:
            res = set()
            for f in self.parents.all():
                res.add(node_to_cache_attr(f))
                res.update(f._get_ancestor(
                    cached_results=cached_results,
                    node_to_cache_attr=node_to_cache_attr
                ))
            cached_results[node_to_cache_attr(self)] = res
            return res

    def get_paths(self, target, use_edges=False, downwards=None):
        try:
            if downwards is None or downwards is True:
                return self._get_paths(target, use_edges=use_edges, downwards=True)
        except NodeNotReachableException as err:
            if downwards is True:
                raise
        return target._get_paths(self, use_edges=use_edges, downwards=False)

    def _get_paths(self, target, use_edges=False, downwards=True):
        if self == target:
            # There can only be 1 zero length path, it also has no edge
            # so we can always return [] for the path
            return [[],]
        if target in self.children.all():
            # If the target is a child of the source object there can only
            # be 1 shortest path
            if use_edges:
                return [ list(self.get_edge_model().objects.filter(
                        child=target,
                        parent=self
                    )), ]
            else:
                return [[target if downwards else self],]

        if target.pk in self.get_descendant_pks():
            paths = []
            path_length = 0
            childItems = self.get_edge_model().objects.filter(
                    parent=self
                )

            for child_edge in childItems:
                if use_edges:
                    node = child_edge
                else:
                    node = child_edge.child if downwards else child_edge.parent
                try:
                    desc_paths = child_edge.child._get_paths(target,
                        use_edges=use_edges,downwards=downwards)
                    desc_path_length = len(desc_paths[0]) + 1
                    if not paths or desc_path_length < path_length:
                        paths = [ [node] + subpath for subpath in desc_paths ]
                        path_length = len(paths[0])
                    elif bool(paths) and desc_path_length == path_length:
                        equal_paths = [ [node] + subpath for subpath in desc_paths ]
                        paths.extend(equal_paths)

                except NodeNotReachableException:
                    pass
        else:
            raise NodeNotReachableException()
        return paths

    def get_roots(self):
        at =  self.get_ancestors_tree()
        roots = set()
        for a in at:
            roots.update(a._get_roots(at[a]))
        return roots or set([self])

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
        dt =  self.get_descendants_tree()
        leaves = set()
        for d in dt:
            leaves.update(d._get_leaves(dt[d]))
        return leaves or set([self])

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
