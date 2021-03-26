import multiprocessing
import unittest

from django.test import TestCase
from django.shortcuts import render_to_response
from django.core.exceptions import ValidationError
from .tree_test_output import expected_tree_output
from ..models.basic import BasicNode, BasicEdge, BasicNodeES, BasicEdgeES

class NodeStorage():
    pass

class DagTestCase(TestCase):
    def setUp(self):
        for i in range(1, 11):
            BasicNode(name="%s" % i).save()

    def test_objects_were_created(self):
        for i in range(1, 11):
            self.assertEqual(BasicNode.objects.get(name="%s" % i).name, "%s" % i)


    @unittest.expectedFailure
    def test_deep_dag(self):
        """
        Create a deep graph and check that graph operations run in a
        reasonable amount of time (linear in size of graph, not
        exponential).
        """
        def run_test():
            # There are on the order of 1 million paths through the graph, so
            # results for intermediate nodes need to be cached
            n = 20

            for i in range(2*n):
                BasicNode(pk=i).save()

            # Create edges
            for i in range(0, 2*n - 2, 2):
                p1 = BasicNode.objects.get(pk=i)
                p2 = BasicNode.objects.get(pk=i+1)
                p3 = BasicNode.objects.get(pk=i+2)
                p4 = BasicNode.objects.get(pk=i+3)

                p1.add_child(p3)
                p1.add_child(p4)
                p2.add_child(p3)
                p2.add_child(p4)

            # Compute descendants of a root node
            BasicNode.objects.get(pk=0).descendants_set()

            # Compute ancestors of a leaf node
            BasicNode.objects.get(pk=2*n - 1).ancestors_set()

            BasicNode.objects.get(pk=0).add_child(BasicNode.objects.get(pk=2*n - 1))

        # Run the test, raising an error if the code times out
        p = multiprocessing.Process(target=run_test)
        p.start()
        p.join(10)
        if p.is_alive():
            p.terminate()
            p.join()
            raise RuntimeError('Graph operations take too long!')

class DagEdgeSaveTests(TestCase):
    """
    Tests requiring the Edges save to return itself
    """
    def setUp(self):
        self.nodes = NodeStorage()
        for i in range(1, 12):
            BasicNodeES(name="%s" % i).save()
            setattr(self.nodes, "p%s" % i, BasicNodeES.objects.get(pk=i))

    def test_correct_edge_is_return_on_add_child(self):
        """Test we return the edge on joining nodes, if the edge's save returns it"""
        edge = self.nodes.p1.add_child(self.nodes.p5)
        self.assertIsInstance(edge, BasicEdgeES)
        self.assertEqual(self.nodes.p1, edge.parent )
        self.assertEqual(self.nodes.p5, edge.child )

    def test_correct_edge_is_return_on_add_parent(self):
        """Test we return the edge on joining nodes, if the edge's save returns it"""
        edge = self.nodes.p5.add_parent(self.nodes.p1)
        self.assertIsInstance(edge, BasicEdgeES)
        self.assertEqual(self.nodes.p1, edge.parent)
        self.assertEqual(self.nodes.p5, edge.child)


class DagRelationshipTests(TestCase):
    def setUp(self):
        self.nodes = NodeStorage()
        for i in range(1, 12):
            BasicNode(name="%s" % i).save()
            setattr(self.nodes, "p%s" % i, BasicNode.objects.get(pk=i))

    def test_can_add_a_child(self):
        """Test we can add a child to a node to form a simple dag/tree"""
        # Creates DAGs
    
        with self.subTest(msg = "as a single tree"):
            self.nodes.p1.add_child(self.nodes.p5)
            # `-- <BasicNode: # 1>
            #     `-- <BasicNode: # 5>
            parents = self.nodes.p5.parents.all()
            children = self.nodes.p1.children.all()
            self.assertIn(self.nodes.p1, parents)
            self.assertIn(self.nodes.p5, children)
            self.assertEqual(len(parents), 1)
            self.assertEqual(len(children), 1)

        with self.subTest(msg = "as a single tree of depth > 1"):
            self.nodes.p5.add_child(self.nodes.p11)
            # `-- <BasicNode: # 1>
            #     `-- <BasicNode: # 5>
            #         `-- <BasicNode: # 11>
            parents = self.nodes.p5.parents.all()
            children = self.nodes.p1.children.all()
            childParents = self.nodes.p11.parents.all()
            grandChildren = self.nodes.p5.children.all()
            self.assertIn(self.nodes.p1, parents)
            self.assertIn(self.nodes.p5, children)
            self.assertIn(self.nodes.p5, childParents)
            self.assertIn(self.nodes.p11, grandChildren)
            self.assertEqual(len(parents), 1)
            self.assertEqual(len(children), 1)
            self.assertEqual(len(childParents), 1)
            self.assertEqual(len(grandChildren), 1)

        with self.subTest(msg = "for multiple independent trees"):
            self.nodes.p2.add_child(self.nodes.p7)
            # |-- <BasicNode: # 1>
            # |   `-- <BasicNode: # 5>
            # |       `-- <BasicNode: # 11>
            # `-- <BasicNode: # 2>
            #     `-- <BasicNode: # 7>
            parents = self.nodes.p5.parents.all()
            children = self.nodes.p1.children.all()
            self.assertIn(self.nodes.p1, parents)
            self.assertIn(self.nodes.p5, children)
            self.assertEqual(len(parents), 1)
            self.assertEqual(len(children), 1)

        with self.subTest(msg = "for multiple independent trees, which share nodes aka a dag"):
            self.nodes.p2.add_child(self.nodes.p5)
            # |-- <BasicNode: # 1>
            # |   `-- <BasicNode: # 5 (clone)>
            # |       `-- <BasicNode: # 11>
            # `-- <BasicNode: # 2>
            #     |-- <BasicNode: # 7>
            #     `-- <BasicNode: # 5 (clone)>

            cloneParents = self.nodes.p5.parents.all()
            cloneChildren = self.nodes.p5.children.all()
            p1Children = self.nodes.p1.children.all()
            p2Children = self.nodes.p2.children.all()
            self.assertIn(self.nodes.p1, cloneParents)
            self.assertIn(self.nodes.p2, cloneParents)
            self.assertEqual(len(cloneParents), 2)
            self.assertIn(self.nodes.p11, cloneChildren)
            self.assertEqual(len(cloneChildren), 1)
            self.assertIn(self.nodes.p5, p1Children)
            self.assertIn(self.nodes.p5, p2Children)
            self.assertEqual(len(p1Children), 1)
            self.assertEqual(len(p2Children), 2)

    def test_we_prevent_circular_refs(self):
        """Test we can prevent adding children to node to form a circular chain"""
        self.nodes.p1.add_child(self.nodes.p5)

        with self.assertRaises(ValidationError) as add_err_cm:
            self.nodes.p5.add_child(self.nodes.p1)
        self.assertEqual(add_err_cm.exception.message, 'The object is an ancestor.')

        with self.assertRaises(ValidationError) as add_err_cm:
            self.nodes.p1.add_parent(self.nodes.p5)
        # # FIXME: we can fix this but is it worth it for the error message
        # self.assertEqual(add_err_cm.exception.message, 'The object is a descendant.')

    def test_we_prevent_self_refs(self):
        """Test we can prevent adding children to node to form a circular chain"""

        with self.subTest(msg="for root node"):
            with self.assertRaises(ValidationError) as add_err_cm:
                self.nodes.p1.add_child(self.nodes.p1)
            self.assertEqual(add_err_cm.exception.message, 'Self links are not allowed.')

        self.nodes.p1.add_child(self.nodes.p5)
        self.nodes.p5.add_child(self.nodes.p7)

        with self.subTest(msg="for child node"):
            with self.assertRaises(ValidationError) as add_err_cm:
                self.nodes.p1.add_child(self.nodes.p1)
            self.assertEqual(add_err_cm.exception.message, 'Self links are not allowed.')

        with self.subTest(msg="for leaf node"):
            with self.assertRaises(ValidationError) as add_err_cm:
                self.nodes.p1.add_child(self.nodes.p1)
            self.assertEqual(add_err_cm.exception.message, 'Self links are not allowed.')

    def test_we_can_set_addition_field_on_edge(self):
        self.nodes.p9.add_child(self.nodes.p10, name='test_name')
        self.assertEqual(
            self.nodes.p9.children.through.objects.filter(child=self.nodes.p10).first().name,
            'test_name')

class DagStructureTests(TestCase):
    def setUp(self,):
        self.nodes = NodeStorage()
        for i in range(1, 12):
            BasicNode(name="%s" % i).save()
            setattr(self.nodes, "p%s" % i, BasicNode.objects.get(pk=i))
        self.build_structure()

    def build_structure(self,):
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

        # `-- <BasicNode: # 1>
        #     |-- <BasicNode: # 5>
        #     |   `-- <BasicNode: # 7>
        #     `-- <BasicNode: # 6>
        #         |-- <BasicNode: # 7>
        #         |-- <BasicNode: # 8>
        #         `-- <BasicNode: # 9>
        #             `-- <BasicNode: # 10>
        # `-- <BasicNode: # 2>
        #     |-- <BasicNode: # 6>
        #     |   |-- <BasicNode: # 7>
        #     |   |-- <BasicNode: # 8>
        #     |   `-- <BasicNode: # 9>
        #     |       `-- <BasicNode: # 10>
        #     `-- <BasicNode: # 8>
        # `-- <BasicNode: # 4>
        #     `-- <BasicNode: # 6>
        #         |-- <BasicNode: # 7>
        #         |-- <BasicNode: # 8>
        #         `-- <BasicNode: # 9>
        #             `-- <BasicNode: # 10>
        # `-- <BasicNode: # 3>
        #     |-- <BasicNode: # 7>
        #     `-- <BasicNode: # 9>
        #         `-- <BasicNode: # 10>


    def test_path_between_nodes(self,):
        #FIXME: what about path 1-7 of ['5', '7']
        self.assertEqual([p.name for p in self.nodes.p1.path(self.nodes.p7)], ['6', '7'])
        self.assertEqual([p.name for p in self.nodes.p1.path(self.nodes.p10)], ['6', '9', '10'])

    def test_distance_between_nodes(self,):
        self.assertEqual(self.nodes.p1.distance(self.nodes.p7), 2)

    def test_can_get_root_and_leaf_nodes_from_node(self,):
        self.assertEqual([p.name for p in self.nodes.p1.get_leaves()], ['8', '10', '7'])
        self.assertEqual([p.name for p in self.nodes.p8.get_roots()], ['1', '2', '4'])

    def test_node_know_if_it_a_root_or_leaf_node(self,):
        self.assertTrue(self.nodes.p1.is_root())
        self.assertFalse(self.nodes.p1.is_leaf())
        self.assertFalse(self.nodes.p10.is_root())
        self.assertTrue(self.nodes.p10.is_leaf())
        self.assertFalse(self.nodes.p6.is_leaf())
        self.assertFalse(self.nodes.p6.is_root())

    def test_can_remove_leaf_child(self):
        """Test we can remove a leaf child node"""
        self.assertTrue(BasicEdge.objects.filter(
            parent=self.nodes.p9,
            child=self.nodes.p10
            ).exists())
        self.nodes.p10.remove_parent(self.nodes.p9)
        self.assertFalse(BasicEdge.objects.filter(
            parent=self.nodes.p9,
            child=self.nodes.p10
            ).exists())
        self.assertTrue(self.nodes.p10.is_island())

    def test_can_remove_root_child(self):
        """Test we can remove a leaf child node"""
        self.assertTrue(BasicEdge.objects.filter(
            parent=self.nodes.p1,
            child=self.nodes.p5
            ).exists())
        self.nodes.p5.remove_parent(self.nodes.p1)
        self.assertFalse(BasicEdge.objects.filter(
            parent=self.nodes.p1,
            child=self.nodes.p5
            ).exists())
        self.assertTrue(self.nodes.p5.is_root())
        self.assertFalse(self.nodes.p2.is_island())
        self.assertIn(self.nodes.p7, self.nodes.p5.children.all())

    def test_can_remove_mid_child(self):
        """Test we can remove a leaf child node"""
        self.assertTrue(BasicEdge.objects.filter(
            parent=self.nodes.p6,
            child=self.nodes.p9
            ).exists())
        self.nodes.p9.remove_parent(self.nodes.p6)
        self.assertFalse(BasicEdge.objects.filter(
            parent=self.nodes.p6,
            child=self.nodes.p9
            ).exists())

        self.assertFalse(self.nodes.p9.is_island())
        self.assertFalse(self.nodes.p6.is_island())

    def test_can_find_ancestors(self):
        self.assertEqual([p.name for p in self.nodes.p6.ancestors_set()], ['1', '2', '4'])

    @unittest.expectedFailure
    def test_dag_tree_render(self):
        # Testing the view
        response = render_to_response('tree.html', { 'dag_list': BasicNode.objects.all()})
        self.assertEqual(response.content.decode('utf-8'), expected_tree_output)
