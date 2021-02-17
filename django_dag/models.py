"""
A class to model hierarchies of objects following
Directed Acyclic Graph structure.

Some ideas stolen from: from https://github.com/stdbrouw/django-treebeard-dag

"""

from django.db import models
from django.core.exceptions import ValidationError


class NodeNotReachableException (Exception):
    """
    Exception for node distance and path
    """
    pass


class NodeBase(object):
    """
    Main node abstract model
    """

    class Meta:
        ordering = ('-id',)

    def __unicode__(self):
        return u"# %s" % self.pk

    def __str__(self):
        return self.__unicode__()

    def add_child(self, descendant, **kwargs):
        """
        Adds a child
        """
        args = kwargs
        args.update({'parent' : self, 'child' : descendant })
        disable_check = args.pop('disable_circular_check', False)
        cls = self.children.through(**kwargs)
        return cls.save(disable_circular_check=disable_check)

    def add_parent(self, parent, *args, **kwargs):
        """
        Adds a parent
        """
        return parent.add_child(self, **kwargs)

    def remove_child(self, descendant):
        """
        Removes a child
        """
        self.children.through.objects.get(parent = self, child = descendant).delete()

    def remove_parent(self, parent):
        """
        Removes a parent
        """
        parent.children.through.objects.get(parent = parent, child = self).delete()


    def descendants_tree(self):
        """
        Returns a tree-like structure with progeny
        """
        tree = {}
        for f in self.children.all():
            tree[f] = f.descendants_tree()
        return tree

    def ancestors_tree(self):
        """
        Returns a tree-like structure with ancestors
        """
        tree = {}
        for f in self.parents.all():
            tree[f] = f.ancestors_tree()
        return tree

    def descendants_set(self, cached_results=None):
        """
        Returns a set of descendants
        """
        if cached_results is None:
            cached_results = dict()
        if self in cached_results.keys():
            return cached_results[self]
        else:
            res = set()
            for f in self.children.all():
                res.add(f)
                res.update(f.descendants_set(cached_results=cached_results))
            cached_results[self] = res
            return res

    def ancestors_set(self, cached_results=None):
        """
        Returns a set of ancestors
        """
        if cached_results is None:
            cached_results = dict()
        if self in cached_results.keys():
            return cached_results[self]
        else:
            res = set()
            for f in self.parents.all():
                res.add(f)
                res.update(f.ancestors_set(cached_results=cached_results))
            cached_results[self] = res
            return res

    # N
    def descendants_edges_set(self, cached_results=None):
        """
        Returns a set of descendants edges
        """
        if cached_results is None:
            cached_results = dict()
        if self in cached_results.keys():
            return cached_results[self]
        else:
            res = set()
            for f in self.children.all():
                res.add((self, f))
                res.update(f.descendants_edges_set(cached_results=cached_results))
            cached_results[self] = res
            return res

    def ancestors_edges_set(self, cached_results=None):
        """
        Returns a set of ancestors edges
        """
        if cached_results is None:
            cached_results = dict()
        if self in cached_results.keys():
            return cached_results[self]
        else:
            res = set()
            for f in self.parents.all():
                res.add((f, self))
                res.update(f.ancestors_edges_set(cached_results=cached_results))
            cached_results[self] = res
            return res

    def nodes_set(self):
        """
        Retrun a set of all nodes
        """
        nodes = set()
        nodes.add(self)
        nodes.update(self.ancestors_set())
        nodes.update(self.descendants_set())
        return nodes

    def edges_set(self):
        """
        Returns a set of all edges
        """
        edges = set()
        edges.update(self.descendants_edges_set())
        edges.update(self.ancestors_edges_set())
        return edges

    def distance(self, target):
        """
        Returns the shortest hops count to the target vertex
        """
        return len(self.path(target))

    def path(self, target):
        """
        Returns the shortest path
        """
        if self == target:
            return []
        if target in self.children.all():
            return [target]
        if target in self.descendants_set():
            path = None
            for d in self.children.all():
                try:
                    desc_path = d.path(target)
                    if not path or len(desc_path) < len(path):
                        path = [d] + desc_path
                except NodeNotReachableException:
                    pass
        else:
            raise NodeNotReachableException
        return path

    def is_root(self):
        """
        Check if has children and not ancestors
        """
        return bool(self.children.exists() and not self.parents.exists())

    def is_leaf(self):
        """
        Check if has ancestors and not children
        """
        return bool(self.parents.exists() and not self.children.exists())

    def is_island(self):
        """
        Check if has no ancestors nor children
        """
        return bool(not self.children.exists() and not self.parents.exists())

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

    def get_roots(self):
        """
        Returns roots nodes, if any
        """
        at =  self.ancestors_tree()
        roots = set()
        for a in at:
            roots.update(a._get_roots(at[a]))
        return roots

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

    def get_leaves(self):
        """
        Returns leaves nodes, if any
        """
        dt =  self.descendants_tree()
        leaves = set()
        for d in dt:
            leaves.update(d._get_leaves(dt[d]))
        return leaves

    @staticmethod
    def circular_checker(parent, child):
        """
        Checks that the object is not an ancestor, avoid self links
        """
        if parent == child:
            raise ValidationError('Self links are not allowed.')
        if child in parent.ancestors_set():
            raise ValidationError('The object is an ancestor.')

    @staticmethod
    def get_node_model(node):
        """
        Get the node mode class.

        This is needed to ensure we are not using the base node class used
        during construction of the model.
        """
        return node._meta.model.children.rel.model


class BaseDagOrderController():
    """
    Interface class to provide support for edge or node ordering.
    """

    def get_node_sequence_field(self, ):
        """
        Returns a single field to be used to support ordering.
        Should the controller require more firlds this can be a primary key
        to another model
        """
        raise NotImplementedError

    def get_edge_sequence_field(self, ):
        """
        Returns a single field to be used to support ordering.
        Should the controller require more firlds this can be a primary key
        to another model
        """
        raise NotImplementedError

    def key_between(self, instance, other):
        """
        Return a key half way between this and other - assuming no other
        intermediate keys exist in the tree.
        """
        raise NotImplementedError

    def next_key(self, instance, parent):
        """
        Provide the next key in the sequence
        """
        raise NotImplementedError

    def first_key(self):
        """
        Provide the first key in the sequence
        """
        raise NotImplementedError

    def get_childsort_query(self, reference_node):
        raise NotImplementedError

    def get_sorted_pos_queryset(self, reference_node, parent=False):
        """
        :returns: A queryset of the nodes
        """
        raise NotImplementedError

    def orderedChildren(self, parent):
        """
        :returns: A queryset of the nodes
        this is slow
        """
        raise NotImplementedError

    def get_first_child(self, reference_node):
        node = self.get_sorted_pos_queryset(
            reference_node
        ).first()
        return node.child if node else None

    def get_last_child(self, reference_node):
        node = self.get_sorted_pos_queryset(
            reference_node
        ).last()
        return node.child if node else None

    def get_first_parent(self, reference_node):
        node = self.get_sorted_pos_queryset(
            reference_node, parent=True
        ).first()
        return node.child if node else None

    def get_last_parent(self, reference_node):
        node = self.get_sorted_pos_queryset(
            reference_node, parent=True
        ).last()
        return node.child if node else None

    def get_next_sibling(self, reference_node, parent_node):
        raise NotImplementedError

    def get_prev_sibling(self, reference_node, parent_node):
        raise NotImplementedError

    def get_first_sibling(self, reference_node, parent_node):
        sibling_node_edge = self.orderedChildren(
            parent_node).first()
        return sibling_node_edge.child if sibling_node_edge else None

    def get_last_sibling(self, reference_node, parent_node):
        sibling_node_edge = self.orderedChildren(
            parent_node).last()
        return sibling_node_edge.child if sibling_node_edge else None

def edge_factory( node_model,
        child_to_field = "id",
        parent_to_field = "id",
        ordering = False,
        concrete = True,
        base_model = models.Model,
    ):
    """
    Dag Edge factory
    """
    try:
        basestring
    except NameError:
        basestring = str
    if isinstance(node_model, basestring):
        try:
            node_model_name = node_model.split('.')[1]
        except IndexError:
            node_model_name = node_model
    else:
        node_model_name = node_model._meta.model_name

    class Edge(base_model):
        class Meta:
            abstract = not concrete

        parent = models.ForeignKey(
            node_model,
            related_name = "%s_child" % node_model_name,
            to_field = parent_to_field,
            on_delete=models.CASCADE
            )
        child = models.ForeignKey(
            node_model,
            related_name = "%s_parent" % node_model_name,
            to_field = child_to_field,
            on_delete=models.CASCADE
            )

        if isinstance(ordering, BaseDagOrderController):
            sequence = ordering.get_edge_sequence_field()
            sequence_manager = ordering
        else:
            sequence_manager = None

        def __unicode__(self):
            return u"%s is child of %s" % (self.child, self.parent)

        def save(self, *args, **kwargs):
            if not kwargs.pop('disable_circular_check', False):
                self.parent.get_node_model(self.parent).circular_checker(self.parent, self.child)
            super(Edge, self).save(*args, **kwargs) # Call the "real" save() method.

    return Edge


def node_factory( edge_model,
        children_null = True,
        base_model = models.Model,
        field = models.ManyToManyField,
        ordering = False,
    ):
    """
    Dag Node factory
    """
    class Node(base_model, NodeBase):
        class Meta:
            abstract = True

        children  = models.ManyToManyField(
                'self',
                blank = children_null,
                symmetrical = False,
                through = edge_model,
                related_name = 'parents')

        if isinstance(ordering, BaseDagOrderController):
            sequence = ordering.get_node_sequence_field()
            sequence_manager = ordering

            def get_first_child(self, ):
                return self.sequence_manager.get_first_child(self)

            def get_last_child(self):
                return self.sequence_manager.get_last_child(self)

            def get_first_parent(self, ):
                return self.sequence_manager.get_first_parent(self)

            def get_last_parent(self):
                return self.sequence_manager.get_last_parent(self)

            @property
            def orderedChildren(self,):
                return self.sequence_manager.orderedChildren(self)

        else:
            sequence_manager = None

    return Node
