import multiprocessing
import unittest

from django.test import TestCase
from django.shortcuts import render_to_response
from django.core.exceptions import ValidationError
from .tree_test_output import expected_tree_output
from ..models.ordered import EdgeOrderedNode, OrderedEdge
from ..models.ordered import OrderedNode, NodeOrderedEdge

from django_dag.exceptions import NodeNotReachableException

class NodeStorage():
    pass

class DagOrderindBasicTests(TestCase):
    def setUp(self):
        self.nodes_eo = NodeStorage()
        for i in range(1, 10):
            EdgeOrderedNode(name="%s" % i).save()
            setattr(self.nodes_eo, "p%s" % i, EdgeOrderedNode.objects.get(pk=i))

        self.nodes_no = NodeStorage()
        for i in range(1, 10):
            EdgeOrderedNode(name="%s" % i).save()
            setattr(self.nodes_no, "p%s" % i, EdgeOrderedNode.objects.get(pk=i))

    def test_can_add_a_child_with_edge_order(self):
        self.nodes_eo.p1.add_child(self.nodes_eo.p5, sequence=12)
        self.nodes_eo.p1.add_child(self.nodes_eo.p6, sequence=8)
        self.nodes_eo.p1.add_child(self.nodes_eo.p7, sequence=4)
        self.nodes_eo.p2.add_child(self.nodes_eo.p5, sequence=1)
        self.nodes_eo.p2.add_child(self.nodes_eo.p6, sequence=7)
        self.nodes_eo.p2.add_child(self.nodes_eo.p7, sequence=5)
        self.assertEqual(OrderedEdge.objects.all().count(), 6)
        self.assertEqual(
            list(OrderedEdge.objects.all().order_by('parent_id','child_id').values_list(
                'parent_id','child_id','sequence')
            ),
            [(1, 5, 12), (1, 6, 8), (1, 7, 4), (2, 5, 1), (2, 6, 7), (2, 7, 5)])

    def test_can_add_a_child_with_overlapping_edge_orders(self):
        self.nodes_eo.p1.add_child(self.nodes_eo.p5, sequence=12)
        self.nodes_eo.p1.add_child(self.nodes_eo.p6, sequence=8)
        self.nodes_eo.p2.add_child(self.nodes_eo.p5, sequence=8)
        self.nodes_eo.p2.add_child(self.nodes_eo.p6, sequence=12)
        self.assertEqual(OrderedEdge.objects.all().count(), 4)
        self.assertEqual(
            list(OrderedEdge.objects.all().order_by('parent_id','child_id').values_list(
                'parent_id','child_id','sequence')
            ),
            [(1, 5, 12), (1, 6, 8), (2, 5, 8), (2, 6, 12)])

    @unittest.skip('No support for add_child setting sequence on node')
    def test_can_add_a_child_with_node_order(self):
        pass


class EdgeSortedDagRelationshipTests(TestCase):
    def setUp(self):
        self.nodes = NodeStorage()
        for i in range(1, 10):
            EdgeOrderedNode(name="%s" % i).save()
            setattr(self.nodes, "p%s" % i, EdgeOrderedNode.objects.get(pk=i))
        # `-- <BasicNode: # 1>
        #     `-- <BasicNode: # 7 o1>
        #     `-- <BasicNode: # 6 o2>
        #     `-- <BasicNode: # 5 o3>
        # `-- <BasicNode: # 2>
        #     `-- <BasicNode: # 5 o1>
        #     `-- <BasicNode: # 7 o2>
        #     `-- <BasicNode: # 6 o3>
        self.nodes.p1.add_child(self.nodes.p5, sequence=12)
        self.nodes.p1.add_child(self.nodes.p6, sequence=8)
        self.nodes.p1.add_child(self.nodes.p7, sequence=4)
        self.nodes.p2.add_child(self.nodes.p5, sequence=1)
        self.nodes.p2.add_child(self.nodes.p6, sequence=8)
        self.nodes.p2.add_child(self.nodes.p7, sequence=4)

    def test_children_ordered_filter(self):
        self.assertEqual(
            list(self.nodes.p1.children.ordered().values_list('pk', '_sequence')),
            [(7,4), (6,8), (5,12)])
        self.assertEqual(
            list(self.nodes.p2.children.ordered().values_list('pk', '_sequence')),
            [(5,1), (7,4), (6,8)])


class NodeSortedDagRelationshipTests(TestCase):
    def setUp(self):
        self.nodes = NodeStorage()
        for i in range(1, 10):
            OrderedNode(name="%s" % i).save()
            setattr(self.nodes, "p%s" % i, OrderedNode.objects.get(pk=i))
        # `-- <BasicNode: # 1>
        #     `-- <BasicNode: # 5 o=1 go=2 >
        #     `-- <BasicNode: # 4 o=2 go=3 >
        #     `-- <BasicNode: # 3 o=3 go=6 >
        # `-- <BasicNode: # 2>
        #     `-- <BasicNode: # 6 o=1 go=1 >
        #     `-- <BasicNode: # 8 o=2 go=4 >
        #     `-- <BasicNode: # 7 o=3 go=5 >
        self.nodes.p1.add_child(self.nodes.p3)
        self.nodes.p1.add_child(self.nodes.p4)
        self.nodes.p1.add_child(self.nodes.p5)
        self.nodes.p2.add_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p7)
        self.nodes.p2.add_child(self.nodes.p8)

        self.nodes.p3.sequence=12
        self.nodes.p4.sequence=6
        self.nodes.p5.sequence=2
        self.nodes.p6.sequence=1
        self.nodes.p7.sequence=11
        self.nodes.p8.sequence=8
        for k, n in self.nodes.__dict__.items():
            if k.startswith('p'):
                n.save()

    def test_children_ordered_filter(self):
        self.assertEqual(
            list(self.nodes.p1.children.ordered().values_list('pk', '_sequence')),
            [(5, 2), (4, 6), (3, 12)])
        self.assertEqual(
            list(self.nodes.p2.children.ordered().values_list('pk', '_sequence')),
            [(6, 1), (8, 8), (7, 11)])


    def test_ordered_filter_on_node(self):
        self.assertEqual(
            list(OrderedNode.objects.ordered().filter(_sequence__gt=0).values_list('pk', '_sequence')),
            [(6, 1), (5, 2), (4, 6), (8, 8), (7, 11), (3, 12)])