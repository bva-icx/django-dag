import multiprocessing
import unittest
from django.conf import settings
from django.db.models import Max
from django.db import models
from django.test import TestCase
from django.template import loader
from django.core.exceptions import ValidationError
from .tree_test_output import expected_tree_output
from ..models.basic import BasicNode, BasicEdge, BasicNodeES, BasicEdgeES
from django_dag.exceptions import NodeNotReachableException
from django_dag.models import (
    node_factory,
    _get_base_manager,
    node_manager_factory,
    BaseNodeManager,
    DagSortOrder,
)

DJANGO_DAG_BACKEND = None
if hasattr(settings, 'DJANGO_DAG_BACKEND'):
    DJANGO_DAG_BACKEND = settings.DJANGO_DAG_BACKEND


class NodeStorage():
    pass


class DagTestCase(TestCase):
    def setUp(self):
        for i in range(1, 11):
            BasicNode(name="%s" % i).save()

    def test_base_manager_management_with_unrelated_managers(self,):
        class UnrelatedManager:
            pass

        class SillyModel:
            _default_manager = UnrelatedManager()

        manager = _get_base_manager(SillyModel, BaseNodeManager)
        self.assertTrue(issubclass(manager, UnrelatedManager))
        self.assertTrue(issubclass(manager, BaseNodeManager))

    def test_node_factory_respects_base_classes(self,):
        # This test checks for early failure in djangocte
        # style models. In those cases at the moment you
        # need to make sure that the QS/ manager derives from
        # the CTE version if you override them. This isn't
        # needed in the std version, so the API is not orthogonal.(FIXME)
        if not hasattr(settings, 'DJANGO_DAG_BACKEND') or not settings.DJANGO_DAG_BACKEND.endswith('djangocte'):
            return

        class MyQS(models.QuerySet):
            pass

        class MyManager(node_manager_factory(BaseNodeManager)):
            pass

        class MyModel(models.Model):
            manager = MyManager

        with self.assertRaises(Exception):
            # This should trigger the assertion in CTEManager.from_queryset()
            class dagmodel(node_factory('Edges', base_model=MyModel,
                                        manager=MyManager,
                                        queryset=MyQS
                                        )):
                pass

        # self.assertTrue(issubclass(MyModel,models.Model))
        # self.assertTrue(issubclass(dagmodel,MyModel))
        # self.assertTrue(isinstance(dagmodel.objects,MyManager))
        # self.assertTrue(isinstance(dagmodel.objects.get_queryset(),MyQS))

    def test_objects_were_created(self):
        for i in range(1, 11):
            self.assertEqual(BasicNode.objects.get(
                name="%s" % i).name, "%s" % i)

    def test_deep_dag(self):
        """
        Create a deep graph and check that graph operations run in a
        reasonable amount of time (linear in size of graph, not
        dxponential).

        """
        def run_test():
            # There are on the order of 1 million paths through the graph, so
            # results for intermediate nodes need to be cached
            n = 20

            for i in range(2 * n):
                BasicNode(pk=i).save()

            # Create edges
            for i in range(0, 2 * n - 2, 2):
                p1 = BasicNode.objects.get(pk=i)
                p2 = BasicNode.objects.get(pk=i + 1)
                p3 = BasicNode.objects.get(pk=i + 2)
                p4 = BasicNode.objects.get(pk=i + 3)

                p1.add_child(p3)
                p1.add_child(p4)
                p2.add_child(p3)
                p2.add_child(p4)

            # Compute descendants of a root node
            BasicNode.objects.get(pk=0).descendants

            # Compute ancestors of a leaf node
            BasicNode.objects.get(pk=2 * n - 1).ancestors

            BasicNode.objects.get(pk=0).add_child(
                BasicNode.objects.get(pk=2 * n - 1))

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
        self.assertEqual(self.nodes.p1, edge.parent)
        self.assertEqual(self.nodes.p5, edge.child)

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

        with self.subTest(msg="as a single tree"):
            self.nodes.p1.add_child(self.nodes.p5)
            # `-- <BasicNode: # 1>
            #     `-- <BasicNode: # 5>
            parents = self.nodes.p5.parents.all()
            children = self.nodes.p1.children.all()
            self.assertIn(self.nodes.p1, parents)
            self.assertIn(self.nodes.p5, children)
            self.assertEqual(len(parents), 1)
            self.assertEqual(len(children), 1)

        with self.subTest(msg="as a single tree of depth > 1"):
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

        with self.subTest(msg="for multiple independent trees"):
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

        with self.subTest(msg="for multiple independent trees, which share nodes aka a dag"):
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
            self.nodes.p9.children.through.objects.filter(
                child=self.nodes.p10).first().name,
            'test_name')


class DagStructureTests(TestCase):
    nodeToTest = BasicNode
    edgeToTest = BasicEdge

    def setUp(self,):
        self.nodes = NodeStorage()
        for i in range(1, 12):
            self.nodeToTest(name="%s" % i).save()
            setattr(self.nodes, "p%s" % i, self.nodeToTest.objects.get(pk=i))
        self.build_structure()

    def build_structure(self,):
        for a in range(0, 20):
            # shift id of edge:
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

    def expand_path(self, paths):
        return[
            [p.name for p in path] for path in paths
        ]

    def test_path_between_nodes_downwards(self,):
        self.assertEqual(
            self.expand_path(self.nodes.p1.get_paths(self.nodes.p7)),
            [
                ['5', '7'],
                ['6', '7'],
            ])
        self.assertEqual(self.expand_path(
            self.nodes.p1.get_paths(self.nodes.p10)), [['6', '9', '10']])
        self.assertEqual(self.expand_path(
            self.nodes.p6.get_paths(self.nodes.p10)), [['9', '10']])
        self.nodes.p3.add_child(self.nodes.p4)
        self.nodes.p3.remove_child(self.nodes.p9)
        self.assertEqual(self.expand_path(
            self.nodes.p4.get_paths(self.nodes.p10)), [['6', '9', '10']])
        self.assertEqual(self.expand_path(self.nodes.p3.get_paths(
            self.nodes.p10)), [['4', '6', '9', '10']])

    def test_paths_Edge_options_with_duplicate_edge(self,):
        self.nodes.p2.add_child(self.nodes.p11)
        self.nodes.p2.add_child(self.nodes.p11)
        paths = self.nodes.p2.get_paths(self.nodes.p11, use_edges=True)

        def check_path(p):
            """check a returned path is what we expect"""
            self.assertEqual(len(p), 1)
            self.assertEqual(p[0].parent, self.nodes.p2)
            self.assertEqual(p[0].child, self.nodes.p11)

        self.assertEqual(len(paths), 2)
        # Check the two paths.
        check_path(paths[0])
        check_path(paths[1])

    def test_paths_node_options_with_duplicate_edge(self,):
        self.nodes.p2.add_child(self.nodes.p11)
        self.nodes.p2.add_child(self.nodes.p11)
        paths = self.nodes.p2.get_paths(self.nodes.p11,)

        # The paths are identical so we don't
        # expect duplicare return
        self.assertEqual(len(paths), 1)
        self.assertEqual(len(paths[0]), 1)
        self.assertEqual(paths[0][0], self.nodes.p11)

    def test_path_raise_for_unattached_nodes(self,):
        with self.assertRaises(NodeNotReachableException) as add_err_cm:  # noqa: F841
            self.nodes.p7.get_paths(self.nodes.p10)

    def test_path_for_same_node_is_empty_list(self,):
        self.assertEqual(
            self.expand_path(self.nodes.p5.get_paths(self.nodes.p5)), [[]]
        )

    def test_path_can_return_edges(self,):
        # Both up and down should be the same
        self.assertEqual(
            [
                (edge.parent.name, edge.child.name)
                for edge in self.nodes.p4.get_paths(self.nodes.p10, use_edges=True)[0]
            ],
            [('4', '6'), ('6', '9'), ('9', '10')]
        )
        self.assertEqual(
            [
                (edge.parent.name, edge.child.name)
                for edge in self.nodes.p10.get_paths(self.nodes.p4, use_edges=True)[0]
            ],
            [('4', '6'), ('6', '9'), ('9', '10')]
        )

    def test_path_between_nodes_upwards(self,):
        self.assertEqual(
            self.expand_path(self.nodes.p7.get_paths(self.nodes.p1)),
            [
                ['1', '5'],
                ['1', '6'],
            ])
        self.assertEqual(self.expand_path(
            self.nodes.p10.get_paths(self.nodes.p1)), [['1', '6', '9']])
        self.assertEqual(self.expand_path(
            self.nodes.p10.get_paths(self.nodes.p6)), [['6', '9']])
        self.nodes.p3.add_child(self.nodes.p4)
        self.nodes.p3.remove_child(self.nodes.p9)
        self.assertEqual(self.expand_path(
            self.nodes.p10.get_paths(self.nodes.p4)), [['4', '6', '9']])
        self.assertEqual(self.expand_path(
            self.nodes.p10.get_paths(self.nodes.p3)), [['3', '4', '6', '9']])

    def test_distance_between_nodes(self,):
        self.assertEqual(self.nodes.p1.distance(self.nodes.p7), 2)
        self.assertEqual(self.nodes.p7.distance(self.nodes.p1), -2)
        self.assertEqual(self.nodes.p7.distance(
            self.nodes.p1, directed=False), 2)

    def test_can_get_root_and_leaf_nodes_from_node(self,):
        with self.subTest("in tree"):
            self.assertEqual(
                sorted([p.name for p in self.nodes.p8.get_roots()], key=int),
                ['1', '2', '4'])
            self.assertEqual(
                sorted([p.name for p in self.nodes.p1.get_leaves()], key=int),
                ['7', '8', '10'])
        with self.subTest("of an island"):
            self.assertEqual(
                [p.name for p in self.nodes.p11.get_roots()], ['11'])
            self.assertEqual(
                [p.name for p in self.nodes.p11.get_leaves()], ['11'])

    def test_can_get_roots_nodes_from_queryset(self,):
        with self.subTest("unfiltered"):
            self.assertEqual(
                sorted([p.name for p in self.nodeToTest.objects.roots()], key=int),
                ['1', '2', '3', '4', '11'])
        with self.subTest("filtered for node"):
            self.assertEqual(
                sorted(
                    [p.name for p in self.nodeToTest.objects.filter(
                        pk__in=[
                            self.nodes.p1.pk,
                            self.nodes.p2.pk,
                            self.nodes.p4.pk,
                            self.nodes.p3.pk,
                            self.nodes.p10.pk,
                            self.nodes.p8.pk
                        ]
                    ).roots(self.nodes.p5)
                    ],
                    key=int),
                ['1'])
            self.assertEqual(
                sorted(
                    [p.name for p in self.nodeToTest.objects.filter(
                        pk__in=[
                            self.nodes.p1.pk,
                            self.nodes.p2.pk,
                            self.nodes.p3.pk,
                            self.nodes.p10.pk,
                            self.nodes.p8.pk
                        ]
                    ).roots(self.nodes.p8)
                    ],
                    key=int),
                ['1', '2'])

    def test_can_get_leaves_nodes_from_queryset(self,):
        with self.subTest("unfiltered"):
            self.assertEqual(
                sorted([p.name for p in self.nodeToTest.objects.leaves()], key=int),
                ['7', '8', '10', '11'])
        with self.subTest("filtered for node"):
            self.assertEqual(
                sorted(
                    [p.name for p in self.nodeToTest.objects.filter(
                        pk__in=[
                            self.nodes.p3.pk,
                            self.nodes.p11.pk,
                            self.nodes.p7.pk,
                            self.nodes.p10.pk,
                            self.nodes.p8.pk,
                        ]
                    ).leaves(self.nodes.p3)
                    ],
                    key=int),
                ['7', '10', ])

    def test_can_get_root_and_leaf_nodes_on_a_lonely_node(self,):
        lonelynode = self.nodeToTest(name="lonely")
        lonelynode.save()
        self.assertEqual([p.name for p in lonelynode.get_leaves()], ['lonely'])
        self.assertEqual([p.name for p in lonelynode.get_roots()], ['lonely'])

    def test_node_know_if_it_a_root_or_leaf_node(self,):
        self.assertTrue(self.nodes.p1.is_root)
        self.assertFalse(self.nodes.p1.is_leaf)
        self.assertFalse(self.nodes.p10.is_root)
        self.assertTrue(self.nodes.p10.is_leaf)
        self.assertFalse(self.nodes.p6.is_leaf)
        self.assertFalse(self.nodes.p6.is_root)

    def test_node_island_also_leaf_and_root(self,):
        self.assertTrue(self.nodes.p11.is_leaf)
        self.assertTrue(self.nodes.p11.is_root)

    def test_can_remove_leaf_child(self):
        """Test we can remove a leaf child node"""
        self.assertTrue(self.edgeToTest.objects.filter(
            parent=self.nodes.p9,
            child=self.nodes.p10
        ).exists())
        self.nodes.p10.remove_parent(self.nodes.p9)
        self.assertFalse(self.edgeToTest.objects.filter(
            parent=self.nodes.p9,
            child=self.nodes.p10
        ).exists())
        self.assertTrue(self.nodes.p10.is_island)

    def test_can_remove_root_child(self):
        """Test we can remove a leaf child node"""
        self.assertTrue(self.edgeToTest.objects.filter(
            parent=self.nodes.p1,
            child=self.nodes.p5
        ).exists())
        self.nodes.p5.remove_parent(self.nodes.p1)
        self.assertFalse(self.edgeToTest.objects.filter(
            parent=self.nodes.p1,
            child=self.nodes.p5
        ).exists())
        self.assertTrue(self.nodes.p5.is_root)
        self.assertFalse(self.nodes.p2.is_island)
        self.assertIn(self.nodes.p7, self.nodes.p5.children.all())

    def test_can_remove_mid_child(self):
        """Test we can remove a leaf child node"""
        self.assertTrue(self.edgeToTest.objects.filter(
            parent=self.nodes.p6,
            child=self.nodes.p9
        ).exists())
        self.nodes.p9.remove_parent(self.nodes.p6)
        self.assertFalse(self.edgeToTest.objects.filter(
            parent=self.nodes.p6,
            child=self.nodes.p9
        ).exists())

        self.assertFalse(self.nodes.p9.is_island)
        self.assertFalse(self.nodes.p6.is_island)

    def test_can_find_ancestors(self):
        self.assertEqual(
            sorted([p.name for p in self.nodes.p6.ancestors], key=int),
            ['1', '2', '4'])

    def test_can_find_descendants(self):
        self.assertEqual(
            sorted([p.name for p in self.nodes.p1.descendants], key=int),
            ['5', '6', '7', '8', '9', '10'])

    def test_can_find_ancestors_pks(self):
        self.assertEqual(
            sorted(self.nodes.p6.get_ancestor_pks()),
            [self.nodes.p1.pk, self.nodes.p2.pk, self.nodes.p4.pk])

    def test_can_find_descendants_pks(self):
        self.assertEqual(
            sorted(self.nodes.p1.get_descendant_pks()),
            [
                self.nodes.p5.pk,
                self.nodes.p6.pk,
                self.nodes.p7.pk,
                self.nodes.p8.pk,
                self.nodes.p9.pk,
                self.nodes.p10.pk
            ])

    @unittest.skip('we need to define the contract')
    def test_ordering_of_descendant_nodes(self):
        # Note: What ordering promises fo we make?
        pass

    @unittest.skip('we need to define the contract')
    def test_ordering_of_ancestor_nodes(self):
        # Note: What ordering promises fo we make?
        pass

    def test_can_find_clan(self):
        self.assertEqual(
            sorted([p.name for p in self.nodes.p5.clan], key=int),
            ['1', '5', '7'])
        self.assertEqual(
            sorted([p.name for p in self.nodes.p6.clan], key=int),
            ['1', '2', '4', '6', '7', '8', '9', '10'])

    def test_can_find_clan_pks(self):
        self.assertEqual(
            sorted(self.nodes.p5.get_clan_pks()),
            [self.nodes.p1.pk, self.nodes.p5.pk, self.nodes.p7.pk])
        self.assertEqual(
            sorted(self.nodes.p6.get_clan_pks()),
            [
                self.nodes.p1.pk,
                self.nodes.p2.pk,
                self.nodes.p4.pk,
                self.nodes.p6.pk,
                self.nodes.p7.pk,
                self.nodes.p8.pk,
                self.nodes.p9.pk,
                self.nodes.p10.pk
            ])

    def test_dag_tree_render(self):
        # Testing the view
        content = loader.render_to_string('django_dag/tree.html', {'dag_list': self.nodeToTest.objects.all()})
        self.assertEqual(content, expected_tree_output)

    @unittest.skip('todo')
    def test_can_move_a_node_between_parents():
        pass

    @unittest.skip('todo')
    def test_cannot_move_a_node_between_parents_causing_circular_ref():
        pass


class NodeCoreSortRelationshipTests(TestCase):

    def setUp(self):
        self.nodes = NodeStorage()
        for i in range(1, 10):
            n = BasicNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        # `-- <BasicNode: # 1>
        #     `-- <BasicNode: # 3 >
        #     `-- <BasicNode: # 4 >
        #     `-- <BasicNode: # 5 >
        # `-- <BasicNode: # 2>
        #     `-- <BasicNode: # 6 >
        #     `-- <BasicNode: # 8 >
        #     `-- <BasicNode: # 7 >
        self.nodes.p1.add_child(self.nodes.p3)
        self.nodes.p1.add_child(self.nodes.p4)
        self.nodes.p1.add_child(self.nodes.p5)
        self.nodes.p2.add_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p7)
        self.nodes.p2.add_child(self.nodes.p8)
        for k, n in self.nodes.__dict__.items():
            if k.startswith('p'):
                n.save()

    def test_with_sort_query_return_nodes(self,):
        qs = BasicNode.objects
        with self.subTest(msg="DagSortOrder BREATH_FIRST"):
            qs_withsort = qs.with_sort_sequence(
                DagSortOrder.NODE_PK,
            )
            map(lambda obj: self.assertIsInstance(
                obj, BasicNode), list(qs_withsort))
        with self.subTest(msg="DagSortOrder DEPTH_FIRST_PREORDER"):
            qs_withsort = qs.with_sort_sequence(
                DagSortOrder.NODE_SEQUENCE,
            )
            map(lambda obj: self.assertIsInstance(
                obj, BasicNode), list(qs_withsort))

    def test_cope_with_sort_with_no_roots_in_base_query(self,):
        self.nodes.p6.add_child(self.nodes.p9)
        self.nodes.p4.add_child(self.nodes.p9)

        qs = BasicNode.objects.filter(pk__in=[3, 4, 5, 7, 8, 9])
        qs_withsort = qs.with_sort_sequence(
            DagSortOrder.NODE_PK,
        )
        qs_sorted = qs_withsort.order_by('dag_pk_path') \
            .values_list('pk', 'dag_pk_path')
        self.assertEqual(
            list(qs_sorted), [
                (3, '0001,0003'),
                (4, '0001,0004'),
                (9, '0001,0004,0009'),
                (5, '0001,0005'),
                (9, '0002,0006,0009'),
                (7, '0002,0007'),
                (8, '0002,0008'),
            ]
        )

    def test_cope_with_sort_on_known_empty_query(self,):
        qs = BasicNode.objects.none()
        qs_withsort = qs.with_sort_sequence(
            DagSortOrder.NODE_PK,
        )
        qs_sorted = qs_withsort.order_by('dag_pk_path')
        self.assertEqual(
            list(qs_sorted), []
        )

    def test_cope_with_sort_on_filter_to_empty_query(self,):
        max_pk = BasicNode.objects.all().aggregate(Max('pk'))
        qs = BasicNode.objects.filter(pk__gt=max_pk['pk__max'])
        qs_withsort = qs.with_sort_sequence(
            DagSortOrder.NODE_PK,
        )
        qs_sorted = qs_withsort.order_by('dag_pk_path')
        self.assertEqual(
            list(qs_sorted), []
        )

    def test_queryset_sortting_filter(self):
        for i in range(10, 16):
            n = BasicNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p4.add_child(self.nodes.p10)
        self.nodes.p4.add_child(self.nodes.p11)
        self.nodes.p10.add_child(self.nodes.p12)
        self.nodes.p7.add_child(self.nodes.p13)

        with self.subTest(msg="with no cloned nodes (path)"):
            qs = BasicNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                DagSortOrder.NODE_PK,
                padsize=2
            ).order_by('dag_pk_path')
            self.assertEqual(
                tuple(qs_sorted.values_list('pk', 'dag_pk_path')),
                (
                    (1, '01'),
                    (3, '01,03'),
                    (4, '01,04'),
                    (10, '01,04,10'),
                    (12, '01,04,10,12'),
                    (11, '01,04,11'),
                    (5, '01,05'),
                    (2, '02'),
                    (6, '02,06'),
                    (7, '02,07'),
                    (13, '02,07,13'),
                    (8, '02,08'),
                    (9, '09'),
                    (14, '14'),
                    (15, '15')
                )
            )

        with self.subTest(msg="with cloned nodes (path)"):
            self.nodes.p6.add_child(self.nodes.p10)
            qs = BasicNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                DagSortOrder.NODE_PK,
                padsize=2
            ).order_by('dag_pk_path')
            self.assertEqual(
                tuple(qs_sorted.values_list('pk', 'dag_pk_path')),
                (
                    (1, '01'),
                    (3, '01,03'),
                    (4, '01,04'),
                    (10, '01,04,10'),
                    (12, '01,04,10,12'),
                    (11, '01,04,11'),
                    (5, '01,05'),
                    (2, '02'),
                    (6, '02,06'),
                    (10, '02,06,10'),
                    (12, '02,06,10,12'),
                    (7, '02,07'),
                    (13, '02,07,13'),
                    (8, '02,08'),
                    (9, '09'),
                    (14, '14'),
                    (15, '15')
                )
            )

        with self.subTest(msg="depth"):
            self.assertEqual(
                tuple(map(lambda x: (x.pk, x.dag_depth), qs_sorted)),
                (
                    (1, 0),
                    (3, 1),
                    (4, 1),
                    (10, 2),
                    (12, 3),
                    (11, 2),
                    (5, 1),
                    (2, 0),
                    (6, 1),
                    (10, 2),
                    (12, 3),
                    (7, 1),
                    (13, 2),
                    (8, 1),
                    (9, 0),
                    (14, 0),
                    (15, 0)
                )
            )
