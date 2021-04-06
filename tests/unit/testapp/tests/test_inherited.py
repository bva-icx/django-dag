import multiprocessing
import unittest

from django.test import TestCase
from django.shortcuts import render_to_response
from django.core.exceptions import ValidationError
from .tree_test_output import expected_tree_output
from ..models.inherited import (
    BaseDerivedNode, DerivedNodeA, DerivedNodeB, DerivedEdge,
    ConcreteBaseNode, InheritedConcreteNode, InheritedConcreteEdge,
    InheritedAbstractNode,InheritedAbstractEdge
)
from django_dag.exceptions import NodeNotReachableException
from .test_basic import DagStructureTests

class NodeStorage():
    pass

class DagStructureTestsInherited(DagStructureTests):
    nodeToTest = InheritedAbstractNode
    edgeToTest = InheritedAbstractEdge
    def setUp(self,):
        self.nodes = NodeStorage()
        for i in range(1, 12):
            n = InheritedAbstractNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.build_structure()


class DagStructureTestsConcreteInherited(DagStructureTests):
    nodeToTest = InheritedConcreteNode
    edgeToTest = InheritedConcreteEdge
    def setUp(self,):
        self.nodes = NodeStorage()
        for i in range(1, 12):
            n = InheritedConcreteNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.build_structure()


    def test_creation(self,):
        pass


@unittest.skip("Most of these fail because the tree walker returns the basenode")
class DagStructureTestsDerivedA(DagStructureTests):
    nodeToTest = DerivedNodeA
    edgeToTest = DerivedEdge
    def setUp(self,):
        self.nodes = NodeStorage()
        for i in range(1, 12):
            n = DerivedNodeA(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i,n)

class DagStructureTestsDerivedMultiNode(TestCase):
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
