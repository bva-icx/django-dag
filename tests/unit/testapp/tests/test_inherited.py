import multiprocessing
import unittest

from django.test import TestCase
from django.shortcuts import render_to_response
from django.core.exceptions import ValidationError
from .tree_test_output import expected_tree_output
from ..models.inherited import (
    BaseDerivedNode, DerivedNodeA, DerivedNodeB, DerivedEdge,
    ConcreteBaseNode, InheritedConcreteNode, InheritedConcreteEdge,
)
from django_dag.exceptions import NodeNotReachableException


class NodeStorage():
    pass


class DagStructureTestsConcreteInherited(TestCase):
    def setUp(self,):
        self.nodes = NodeStorage()
        for i in range(1, 12):
            InheritedConcreteNode(name="%s" % i).save()
            setattr(self.nodes, "p%s" % i, InheritedConcreteNode.objects.get(pk=i))
        self.build_structure()

    def build_structure(self,):
        for a in range(0,20):
            # Shift id of edge:
            self.nodes.p1.add_child(self.nodes.p2)
            self.nodes.p1.remove_child(self.nodes.p2)
        self.nodes.p1.add_child(self.nodes.p5)
        self.nodes.p5.add_child(self.nodes.p7)
        self.nodes.p1.add_child(self.nodes.p6)
        self.nodes.p6.add_child(self.nodes.p7)
        self.nodes.p2.add_child(self.nodes.p6)
        self.nodes.p3.add_child(self.nodes.p7)
        self.nodes.p6.add_child(self.nodes.p8)
        self.nodes.p2.add_child(self.nodes.p8)
        self.nodes.p6.add_parent(self.nodes.p4)
        self.nodes.p9.add_parent(self.nodes.p3)
        self.nodes.p9.add_parent(self.nodes.p6)
        self.nodes.p9.add_child(self.nodes.p10)

    def test_creation(self,):
        pass

class DagStructureTestsDerived(TestCase):
    def setUp(self,):
        self.nodes = NodeStorage()
        for i in range(1, 12):
            DerivedNodeA(name="%s" % i).save()
            setattr(self.nodes, "pA%s" % i, DerivedNodeA.objects.get(pk=i))
        for i in range(12, 24):
            DerivedNodeB(name="%s" % i).save()
            setattr(self.nodes, "pB%s" % i, DerivedNodeB.objects.get(pk=i))
        self.build_structure()

    def build_structure(self,):
        for a in range(0,20):
            # Shift id of edge:
            self.nodes.pA1.add_child(self.nodes.pA2)
            self.nodes.pA1.remove_child(self.nodes.pA2)

        self.nodes.pA1.add_child(self.nodes.pA5)
        self.nodes.pA5.add_child(self.nodes.pA7)
        self.nodes.pA1.add_child(self.nodes.pA6)
        self.nodes.pA6.add_child(self.nodes.pA7)

        self.nodes.pA2.add_child(self.nodes.pA6)
        self.nodes.pA3.add_child(self.nodes.pA7)
        self.nodes.pA6.add_child(self.nodes.pA8)
        self.nodes.pA2.add_child(self.nodes.pA8)

        self.nodes.pA6.add_parent(self.nodes.pA4)
        self.nodes.pA9.add_parent(self.nodes.pA3)
        self.nodes.pA9.add_parent(self.nodes.pA6)
        self.nodes.pA9.add_child(self.nodes.pA10)

    def test_model_node_type(self,):
        self.assertEqual(
            self.nodes.pA1.get_node_model(),
            BaseDerivedNode
        )
        self.assertEqual(
            self.nodes.pB12.get_node_model(),
            BaseDerivedNode
        )

    def test_model_edge_type(self,):
        self.assertEqual(
            self.nodes.pA1.get_edge_model(),
            DerivedEdge
        )
        self.assertEqual(
            self.nodes.pB12.get_edge_model(),
            DerivedEdge
        )
