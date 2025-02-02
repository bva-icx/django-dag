from django_dag.models.order_control import Position
import unittest
from django.conf import settings
from django.test import TestCase
from django.db import NotSupportedError
from django.db.models import TextField
from django.db.models.expressions import F, Value
from django.db.models.functions import Cast, Concat, RPad

from ..models.ordered import EdgeOrderedNode, OrderedEdge
from ..models.ordered import OrderedNode

from django_dag.exceptions import InvalidNodeMove
from django_dag.models import DagSortOrder

DJANGO_DAG_BACKEND = None
if hasattr(settings, 'DJANGO_DAG_BACKEND'):
    DJANGO_DAG_BACKEND = settings.DJANGO_DAG_BACKEND


class NodeStorage():
    pass


class DagOrderingBasicTests(TestCase):
    def setUp(self):
        self.nodes_eo = NodeStorage()
        for i in range(1, 10):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes_eo, "p%s" % i, n)

        self.nodes_no = NodeStorage()
        for i in range(1, 10):
            e = EdgeOrderedNode(name="%s" % i)
            e.save()
            setattr(self.nodes_no, "p%s" % i, e)

    def test_can_add_a_child_with_edge_order(self):
        self.nodes_eo.p1.add_child(self.nodes_eo.p5, sequence=12)
        self.nodes_eo.p1.add_child(self.nodes_eo.p6, sequence=8)
        self.nodes_eo.p1.add_child(self.nodes_eo.p7, sequence=4)
        self.nodes_eo.p2.add_child(self.nodes_eo.p5, sequence=1)
        self.nodes_eo.p2.add_child(self.nodes_eo.p6, sequence=7)
        self.nodes_eo.p2.add_child(self.nodes_eo.p7, sequence=5)
        self.assertEqual(OrderedEdge.objects.all().count(), 6)
        self.assertEqual(
            list(OrderedEdge.objects.all().order_by('parent__name', 'child__name').values_list(
                'parent__name', 'child__name', 'sequence')
            ),
            [('1', '5', 12), ('1', '6', 8), ('1', '7', 4), ('2', '5', 1), ('2', '6', 7), ('2', '7', 5)])

    def test_can_add_a_child_with_overlapping_edge_orders(self):
        self.nodes_eo.p1.add_child(self.nodes_eo.p5, sequence=12)
        self.nodes_eo.p1.add_child(self.nodes_eo.p6, sequence=8)
        self.nodes_eo.p2.add_child(self.nodes_eo.p5, sequence=8)
        self.nodes_eo.p2.add_child(self.nodes_eo.p6, sequence=12)
        self.assertEqual(OrderedEdge.objects.all().count(), 4)
        self.assertEqual(
            list(OrderedEdge.objects.all().order_by('parent__name', 'child__name').values_list(
                'parent__name', 'child__name', 'sequence')
            ),
            [('1', '5', 12), ('1', '6', 8), ('2', '5', 8), ('2', '6', 12)])

    @unittest.skip('No support for add_child setting sequence on node')
    def test_can_add_a_child_with_node_order(self):
        pass

    def test_can_use_node_insert_after_at_ends_uses_key_next(self):
        self.nodes_eo.p1.add_child(self.nodes_eo.p6, sequence=8)
        self.nodes_eo.p1.add_child(self.nodes_eo.p7, sequence=4)
        self.nodes_eo.p2.add_child(self.nodes_eo.p5, sequence=1)
        self.nodes_eo.p2.add_child(self.nodes_eo.p6, sequence=7)
        self.nodes_eo.p2.add_child(self.nodes_eo.p7, sequence=5)

        self.nodes_eo.p1.insert_child_after(self.nodes_eo.p5, self.nodes_eo.p6)

        self.assertEqual(OrderedEdge.objects.all().count(), 6)
        self.assertEqual(
            list(OrderedEdge.objects.all().order_by('parent__name', 'child__name').values_list(
                'parent__name', 'child__name', 'sequence')
            ),
            [('1', '5', 54), ('1', '6', 8), ('1', '7', 4), ('2', '5', 1), ('2', '6', 7), ('2', '7', 5)])

    def test_can_use_node_insert_before_at_start_uses_key_next(self):
        self.nodes_eo.p1.add_child(self.nodes_eo.p5, sequence=12)
        self.nodes_eo.p1.add_child(self.nodes_eo.p6, sequence=8)
        self.nodes_eo.p2.add_child(self.nodes_eo.p5, sequence=1)
        self.nodes_eo.p2.add_child(self.nodes_eo.p6, sequence=7)
        self.nodes_eo.p2.add_child(self.nodes_eo.p7, sequence=5)

        self.nodes_eo.p1.insert_child_before(
            self.nodes_eo.p7, self.nodes_eo.p6)

        self.assertEqual(OrderedEdge.objects.all().count(), 6)
        self.assertEqual(
            list(OrderedEdge.objects.all().order_by('parent__name', 'child__name').values_list(
                'parent__name', 'child__name', 'sequence')
            ),
            [('1', '5', 12), ('1', '6', 8), ('1', '7', 4), ('2', '5', 1), ('2', '6', 7), ('2', '7', 5)])

    @unittest.skip('no exception or test written as yet')
    def test_can_use_node_insert_after_can_cause_renumbering(self):
        pass

    @unittest.skip('no exception or test written as yet')
    def test_can_use_node_insert_after_raises_is_not_attached(self):
        pass

    @unittest.skip('no exception or test written as yet')
    def test_can_use_node_insert_before_raises_is_not_attached(self):
        pass

    def test_can_use_node_insert_after_uses_key_between(self):
        self.nodes_eo.p1.add_child(self.nodes_eo.p5, sequence=12)
        self.nodes_eo.p1.add_child(self.nodes_eo.p7, sequence=4)
        self.nodes_eo.p2.add_child(self.nodes_eo.p5, sequence=1)
        self.nodes_eo.p2.add_child(self.nodes_eo.p6, sequence=7)
        self.nodes_eo.p2.add_child(self.nodes_eo.p7, sequence=5)
        self.nodes_eo.p1.insert_child_after(self.nodes_eo.p6, self.nodes_eo.p7)
        self.assertEqual(OrderedEdge.objects.all().count(), 6)
        self.assertEqual(
            list(OrderedEdge.objects.all().order_by('parent__name', 'child__name').values_list(
                'parent__name', 'child__name', 'sequence')
            ),
            [('1', '5', 12), ('1', '6', 8), ('1', '7', 4), ('2', '5', 1), ('2', '6', 7), ('2', '7', 5)])

    def test_can_use_node_insert_before_uses_key_between(self):
        self.nodes_eo.p1.add_child(self.nodes_eo.p5, sequence=12)
        self.nodes_eo.p1.add_child(self.nodes_eo.p7, sequence=4)
        self.nodes_eo.p2.add_child(self.nodes_eo.p5, sequence=1)
        self.nodes_eo.p2.add_child(self.nodes_eo.p6, sequence=7)
        self.nodes_eo.p2.add_child(self.nodes_eo.p7, sequence=5)
        self.nodes_eo.p1.insert_child_before(
            self.nodes_eo.p6, self.nodes_eo.p5)
        self.assertEqual(OrderedEdge.objects.all().count(), 6)
        self.assertEqual(
            list(OrderedEdge.objects.all().order_by('parent__name', 'child__name').values_list(
                'parent__name', 'child__name', 'sequence')
            ),
            [('1', '5', 12), ('1', '6', 8), ('1', '7', 4), ('2', '5', 1), ('2', '6', 7), ('2', '7', 5)])


class EdgeSortedDagRelationshipTests(TestCase):
    def setUp(self):
        self.nodes = NodeStorage()
        for i in range(1, 10):
            e = EdgeOrderedNode(name="%s" % i)
            e.save()
            setattr(self.nodes, "p%s" % i, e)
        # `-- <BasicNode: # 1>
        #     `--  4 -- <BasicNode: # 7 o1>
        #     `--  8 -- <BasicNode: # 6 o2>
        #     `-- 12 -- <BasicNode: # 5 o3>
        # `-- <BasicNode: # 2>
        #     `--  1 -- <BasicNode: # 5 o1>
        #     `--  4 -- <BasicNode: # 7 o2>
        #     `--  8 -- <BasicNode: # 6 o3>

        self.nodes.p1.add_child(self.nodes.p5, sequence=12)
        self.nodes.p1.add_child(self.nodes.p6, sequence=8)
        self.nodes.p1.add_child(self.nodes.p7, sequence=4)
        self.nodes.p2.add_child(self.nodes.p5, sequence=1)
        self.nodes.p2.add_child(self.nodes.p6, sequence=8)
        self.nodes.p2.add_child(self.nodes.p7, sequence=4)

    def test_queryset_sortting_filter_mixed(self):
        for i in range(10, 16):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p6.insert_child_after(self.nodes.p10, None)
        self.nodes.p6.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p2.remove_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p3, sequence=9)
        self.nodes.p3.add_child(self.nodes.p13, sequence=6)

        with self.subTest(msg="with no cloned nodes"):
            qs = EdgeOrderedNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                DagSortOrder.NODE_PK,
                padsize=2,
            )
            qs_sorted = qs_sorted.with_sort_sequence(
                DagSortOrder.NODE_SEQUENCE,
                padsize=2,
            ).order_by('dag_pk_path')
            self.assertEqual(
                tuple(
                    qs_sorted.values_list(
                        'pk',
                        'dag_sequence_path',
                        'dag_pk_path',
                    )
                ),
                (
                    (1, '01', '01'),
                    (5, '01,12', '01,05'),
                    (6, '01,08', '01,06'),
                    (10, '01,08,50', '01,06,10'),
                    (12, '01,08,50,50', '01,06,10,12'),
                    (11, '01,08,75', '01,06,11'),
                    (7, '01,04', '01,07'),
                    (2, '02', '02'),
                    (3, '02,09', '02,03'),
                    (13, '02,09,06', '02,03,13'),
                    (5, '02,01', '02,05'),
                    (7, '02,04', '02,07'),
                    (4, '04', '04'),
                    (8, '08', '08'),
                    (9, '09', '09'),
                    (14, '14', '14'),
                    (15, '15', '15')
                )
            )

    def test_queryset_sortting_filter_node_distinct(self):
        for i in range(10, 16):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p6.insert_child_after(self.nodes.p10, None)
        self.nodes.p6.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p2.remove_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p3, sequence=9)
        self.nodes.p3.add_child(self.nodes.p13, sequence=6)
        expected_nodes = (
            (1, '0001',),
            (5, '0001,0005'),
            (6, '0001,0006'),
            (10, '0001,0006,0010'),
            (12, '0001,0006,0010,0012'),
            (11, '0001,0006,0011'),
            (7, '0001,0007'),
            (2, '0002'),
            (3, '0002,0003'),
            (13, '0002,0003,0013'),
            (4, '0004'),
            (8, '0008'),
            (9, '0009'),
            (14, '0014'),
            (15, '0015')
        )
        qs = EdgeOrderedNode.objects.all()
        with self.subTest(msg="query sort order same as visit sort"):
            qs_sorted = qs.with_sort_sequence(
                padsize=2,
            ).distinct_node(
                'dag_node_path'
            ).order_by('dag_node_path')
            self.assertEqual(
                tuple(qs_sorted.values_list('pk', 'dag_node_path')),
                expected_nodes
            )
        with self.subTest(msg="query sort order reverse to filter order"):
            qs_sorted = qs.with_sort_sequence(
                padsize=2,
            ).distinct_node(
                'dag_node_path'
            ).order_by('-dag_node_path')
            self.assertEqual(
                tuple(qs_sorted.values_list('pk', 'dag_node_path')),
                tuple(reversed(expected_nodes))
            )

    def test_queryset_sortting_filter_node_distinct_returns_nodes(self):
        for i in range(10, 16):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p6.insert_child_after(self.nodes.p10, None)
        self.nodes.p6.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p2.remove_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p3, sequence=9)
        self.nodes.p3.add_child(self.nodes.p13, sequence=6)
        qs = EdgeOrderedNode.objects.all()
        qs_sorted = qs.with_sort_sequence(
            padsize=2,
        ).distinct_node('dag_node_path')
        for node in qs_sorted:
            self.assertIsInstance(node, EdgeOrderedNode)

    def test_queryset_sortting_filter_node_distinct_without_sort(self):
        for i in range(10, 16):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p6.insert_child_after(self.nodes.p10, None)
        self.nodes.p6.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p2.remove_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p3, sequence=9)
        self.nodes.p3.add_child(self.nodes.p13, sequence=6)
        qs = EdgeOrderedNode.objects.all()
        with self.assertRaises(NotSupportedError):
            list(qs.distinct_node('dag_node_path'))

    def test_queryset_sortting_filter_breathfirst(self):
        for i in range(10, 16):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p6.insert_child_after(self.nodes.p10, None)
        self.nodes.p6.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p2.remove_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p3, sequence=9)
        self.nodes.p3.add_child(self.nodes.p13, sequence=6)

        with self.subTest(msg="with no cloned nodes"):
            qs = EdgeOrderedNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                padsize=2,
            ).order_by('dag_depth', 'dag_sequence_path')
            self.assertEqual(
                tuple(
                    map(
                        lambda row: row[: 2],
                        qs_sorted.values_list('pk', 'dag_sequence_path', 'dag_depth')
                    )
                ),
                (
                    (1, '01'),
                    (2, '02'),
                    (4, '04'),
                    (8, '08'),
                    (9, '09'),
                    (14, '14'),
                    (15, '15'),
                    (7, '01,04'),
                    (6, '01,08'),
                    (5, '01,12'),
                    (5, '02,01'),
                    (7, '02,04'),
                    (3, '02,09'),
                    (10, '01,08,50'),
                    (11, '01,08,75'),
                    (13, '02,09,06'),
                    (12, '01,08,50,50'),
                )
            )

    def test_queryset_sortting_filter_pk_path(self):
        for i in range(10, 16):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p6.insert_child_after(self.nodes.p10, None)
        self.nodes.p6.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p2.remove_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p3, sequence=9)
        self.nodes.p3.add_child(self.nodes.p13, sequence=6)

        with self.subTest(msg="with no cloned nodes"):
            qs = EdgeOrderedNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                DagSortOrder.NODE_PK,
                padsize=2,
            ).order_by('dag_pk_path')

            self.assertEqual(
                tuple(qs_sorted.values_list('pk', 'dag_pk_path')),
                (
                    (1, '01',),
                    (5, '01,05'),
                    (6, '01,06'),
                    (10, '01,06,10'),
                    (12, '01,06,10,12'),
                    (11, '01,06,11'),
                    (7, '01,07'),
                    (2, '02'),
                    (3, '02,03'),
                    (13, '02,03,13'),
                    (5, '02,05'),
                    (7, '02,07'),
                    (4, '04'),
                    (8, '08'),
                    (9, '09'),
                    (14, '14'),
                    (15, '15')
                )
            )

    def test_queryset_sortting_filter_default(self):
        for i in range(10, 16):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p6.insert_child_after(self.nodes.p10, None)
        self.nodes.p6.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p2.remove_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p3, sequence=9)
        self.nodes.p3.add_child(self.nodes.p13, sequence=6)

        with self.subTest(msg="with no cloned nodes"):
            qs = EdgeOrderedNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                padsize=2,
            ).order_by('dag_sequence_path')
            self.assertEqual(
                tuple(qs_sorted.values_list('pk', 'dag_sequence_path')),
                (
                    (1, '01'),
                    (7, '01,04'),
                    (6, '01,08'),
                    (10, '01,08,50'),
                    (12, '01,08,50,50'),
                    (11, '01,08,75'),
                    (5, '01,12'),
                    (2, '02'),
                    (5, '02,01'),
                    (7, '02,04'),
                    (3, '02,09'),
                    (13, '02,09,06'),
                    (4, '04'),
                    (8, '08'),
                    (9, '09'),
                    (14, '14'),
                    (15, '15')
                )
            )

    def test_queryset_sortting_filter_depthfirst_preorder(self):
        for i in range(10, 16):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p6.insert_child_after(self.nodes.p10, None)
        self.nodes.p6.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p2.remove_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p3, sequence=9)
        self.nodes.p3.add_child(self.nodes.p13, sequence=6)

        with self.subTest(msg="with no cloned nodes"):
            qs = EdgeOrderedNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                DagSortOrder.NODE_SEQUENCE,
                padsize=2,
            ).order_by('dag_sequence_path')
            self.assertEqual(
                tuple(qs_sorted.values_list('pk', 'dag_sequence_path')),
                (
                    (1, '01'),
                    (7, '01,04'),
                    (6, '01,08'),
                    (10, '01,08,50'),
                    (12, '01,08,50,50'),
                    (11, '01,08,75'),
                    (5, '01,12'),
                    (2, '02'),
                    (5, '02,01'),
                    (7, '02,04'),
                    (3, '02,09'),
                    (13, '02,09,06'),
                    (4, '04'),
                    (8, '08'),
                    (9, '09'),
                    (14, '14'),
                    (15, '15')
                )
            )
        with self.subTest(msg="with cloned nodes"):
            self.nodes.p2.insert_child_after(self.nodes.p6, self.nodes.p5)
            qs = EdgeOrderedNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                DagSortOrder.NODE_SEQUENCE,
                padsize=2,
            ).order_by('dag_sequence_path')
            self.assertEqual(
                tuple(qs_sorted.values_list('pk', 'dag_sequence_path')),
                (
                    (1, '01'),
                    (7, '01,04'),
                    (6, '01,08'),
                    (10, '01,08,50'),
                    (12, '01,08,50,50'),
                    (11, '01,08,75'),
                    (5, '01,12'),
                    (2, '02'),
                    (5, '02,01'),
                    (6, '02,02'),
                    (10, '02,02,50'),
                    (12, '02,02,50,50'),
                    (11, '02,02,75'),
                    (7, '02,04'),
                    (3, '02,09'),
                    (13, '02,09,06'),
                    (4, '04'),
                    (8, '08'),
                    (9, '09'),
                    (14, '14'),
                    (15, '15')
                )
            )

    def test_queryset_sortting_filter_depthfirst_postorder(self):
        for i in range(10, 16):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p6.insert_child_after(self.nodes.p10, None)
        self.nodes.p6.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p2.remove_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p3, sequence=9)
        self.nodes.p3.add_child(self.nodes.p13, sequence=6)

        with self.subTest(msg="with no cloned nodes"):
            qs = EdgeOrderedNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                padsize=2,
            ).annotate(
                dag_postorder_path=RPad(
                    Concat(
                        Cast(F('dag_sequence_path'), output_field=TextField()),
                        Value(','),
                    ),
                    (2 + 1) * 5,
                    Value('A')
                )
            ).order_by('dag_postorder_path')
            self.assertEqual(
                tuple(
                    map(
                        lambda row: row[: 2],
                        qs_sorted.values_list('pk', 'dag_sequence_path', 'dag_postorder_path')
                    )
                ),
                (
                    (7, '01,04'),
                    (12, '01,08,50,50'),
                    (10, '01,08,50'),
                    (11, '01,08,75'),
                    (6, '01,08'),
                    (5, '01,12'),
                    (1, '01'),
                    (5, '02,01'),
                    (7, '02,04'),
                    (13, '02,09,06'),
                    (3, '02,09'),
                    (2, '02'),
                    (4, '04'),
                    (8, '08'),
                    (9, '09'),
                    (14, '14'),
                    (15, '15'),
                )
            )

    @unittest.skip('no exception or test written as yet')
    def test_queryset_sortting_filter_depthfirst_inorder(self):
        pass

    def test_children_ordered_filter(self):
        self.assertEqual(
            list(self.nodes.p1.children
                 .with_sequence().order_by('sequence')
                 .values_list('name', 'sequence')),
            [('7', 4), ('6', 8), ('5', 12)])
        self.assertEqual(
            list(self.nodes.p2.children
                 .with_sequence().order_by('sequence')
                 .values_list('name', 'sequence')),
            [('5', 1), ('7', 4), ('6', 8)])

    def test_parent_ordered_filter(self):
        self.assertEqual(
            list(self.nodes.p5.parents
                 .with_sequence().order_by('sequence')
                 .values_list('name', 'sequence')),
            [('2', 1), ('1', 12)])

    def test_parent_ordered_filter_alternatename(self):
        self.assertEqual(
            list(self.nodes.p5.parents
                 .with_sequence(fieldname='alternate').order_by('alternate')
                 .values_list('name', 'alternate')),
            [('2', 1), ('1', 12)])

    def test_can_get_first_child_of_node(self):
        self.assertEqual(self.nodes.p1.get_first_child(), self.nodes.p7)
        self.assertEqual(self.nodes.p2.get_first_child(), self.nodes.p5)
        self.assertEqual(
            self.nodes.p2.get_first_child(),
            self.nodes.p2.children.with_sequence().order_by('sequence').first(),
        )
        self.assertEqual(self.nodes.p6.get_first_child(), None)

    def test_can_get_last_child_of_node(self):
        self.assertEqual(self.nodes.p1.get_last_child(), self.nodes.p5)
        self.assertEqual(self.nodes.p2.get_last_child(), self.nodes.p6)
        self.assertEqual(
            self.nodes.p2.get_last_child(),
            self.nodes.p2.children.with_sequence().order_by('sequence').last(),
        )
        self.assertEqual(self.nodes.p6.get_last_child(), None)

    def test_can_get_first_parent_of_node(self):
        self.assertEqual(self.nodes.p5.get_first_parent(), self.nodes.p2)
        # # FIXME: what should dup sequences reveal
        # self.assertEqual(self.nodes.p6.get_first_parent(), self.nodes.p1)
        self.assertEqual(
            self.nodes.p5.get_first_parent(),
            self.nodes.p5.parents.with_sequence().order_by('sequence').first(),
        )

    def test_can_get_last_parent_of_node(self):
        self.assertEqual(self.nodes.p5.get_last_parent(), self.nodes.p1)
        # # FIXME: what should dup sequences reveal
        # self.assertEqual(self.nodes.p6.get_first_parent(), self.nodes.p1)
        self.assertEqual(
            self.nodes.p5.get_last_parent(),
            self.nodes.p5.parents.with_sequence().order_by('sequence').last(),
        )

    def test_can_get_next_sibling_of_node(self):
        self.assertEqual(
            self.nodes.p6.get_next_sibling(self.nodes.p1),
            self.nodes.p5)
        self.assertEqual(self.nodes.p5.get_next_sibling(self.nodes.p1), None)
        self.assertEqual(
            self.nodes.p5.get_next_sibling(self.nodes.p2),
            self.nodes.p7)
        self.assertEqual(
            self.nodes.p6.get_next_sibling(self.nodes.p2), None)

    def test_can_get_prev_sibling_of_node(self):
        self.assertEqual(self.nodes.p7.get_prev_sibling(self.nodes.p1), None)
        self.assertEqual(
            self.nodes.p5.get_prev_sibling(self.nodes.p1),
            self.nodes.p6)
        self.assertEqual(
            self.nodes.p7.get_prev_sibling(self.nodes.p2),
            self.nodes.p5)
        self.assertEqual(
            self.nodes.p5.get_prev_sibling(self.nodes.p2), None)

    def test_cannot_move_a_node_between_parents_causing_circular_ref(self):
        self.nodes.p5.add_child(self.nodes.p9, sequence=12)
        with self.assertRaises(InvalidNodeMove):
            self.nodes.p1.move_node(
                None,
                self.nodes.p9,
            )

    def test_can_move_a_node_between_parents_default_location(self):
        self.nodes.p2.add_child(self.nodes.p9, sequence=12)
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p2.pk]
        )
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p1,
        )
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p1.pk]
        )

    def test_can_move_a_node_between_parents_first(self):
        self.nodes.p2.add_child(self.nodes.p9, sequence=12)
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p2.pk]
        )
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p1,
            position=Position.FIRST
        )
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p1.pk]
        )
        self.assertEqual(
            self.nodes.p1.children
                .with_sequence().order_by('sequence')
                .values_list('pk', flat=True).first(),
            self.nodes.p9.pk
        )

    def test_can_move_a_sibling_node_to_be_first(self):
        self.nodes.p1.add_child(self.nodes.p9, sequence=14)
        self.nodes.p9.move_node(
            self.nodes.p1,
            self.nodes.p1,
            position=Position.FIRST
        )
        self.assertEqual(
            self.nodes.p1.children
                .with_sequence().order_by('sequence')
                .values_list('pk', flat=True).first(),
            self.nodes.p9.pk
        )

    def test_can_move_a_root_node_to_be_firstchild_of_another_node(self):
        self.nodes.p9.move_node(
            None,
            self.nodes.p1,
            position=Position.FIRST
        )
        self.assertEqual(
            self.nodes.p1.children
                .with_sequence().order_by('sequence')
                .values_list('pk', flat=True).first(),
            self.nodes.p9.pk
        )

    def test_can_move_a_node_between_parents_last(self):
        self.nodes.p2.add_child(self.nodes.p9, sequence=12)
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p2.pk]
        )
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p1,
            position=Position.LAST
        )
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p1.pk]
        )
        self.assertEqual(
            self.nodes.p1.children
                .with_sequence().order_by('sequence')
                .values_list('pk', flat=True).last(),
            self.nodes.p9.pk
        )

    def test_can_move_a_sibling_node_to_be_last(self):
        self.nodes.p1.add_child(self.nodes.p9, sequence=2)
        self.nodes.p9.move_node(
            self.nodes.p1,
            self.nodes.p1,
            position=Position.LAST
        )
        self.assertEqual(
            self.nodes.p1.children
                .with_sequence().order_by('sequence')
                .values_list('pk', flat=True).last(),
            self.nodes.p9.pk
        )

    def test_can_move_a_root_node_to_be_lastchild_of_another_node(self):
        self.nodes.p9.move_node(
            None,
            self.nodes.p1,
            position=Position.LAST
        )
        self.assertEqual(
            self.nodes.p1.children
                .with_sequence().order_by('sequence')
                .values_list('pk', flat=True).last(),
            self.nodes.p9.pk
        )

    def test_can_move_a_node_between_parents_before(self):
        self.nodes.p2.add_child(self.nodes.p9, sequence=12)
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p2.pk]
        )
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p1,
            destination_sibling=self.nodes.p6,
            position=Position.BEFORE
        )
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p1.pk]
        )
        self.assertEqual(
            list(self.nodes.p1.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p7.pk, self.nodes.p9.pk,
                self.nodes.p6.pk, self.nodes.p5.pk]
        )

    def test_can_move_a_node_before_a_sibling(self):
        self.nodes.p2.add_child(self.nodes.p9, sequence=12)
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p2,
            destination_sibling=self.nodes.p7,
            position=Position.BEFORE
        )
        self.assertEqual(
            list(self.nodes.p2.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p5.pk, self.nodes.p9.pk,
                self.nodes.p7.pk, self.nodes.p6.pk]
        )

    def test_can_move_a_node_before_a_sibling_quickapi(self):
        self.nodes.p2.add_child(self.nodes.p9, sequence=12)
        self.nodes.p2.move_child_before(
            self.nodes.p9,
            self.nodes.p7,
        )
        self.assertEqual(
            list(self.nodes.p2.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p5.pk, self.nodes.p9.pk,
                self.nodes.p7.pk, self.nodes.p6.pk]
        )

    def test_can_move_a_node_between_parents_after(self):
        self.nodes.p2.add_child(self.nodes.p9, sequence=12)
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p2.pk]
        )
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p1,
            destination_sibling=self.nodes.p6,
            position=Position.AFTER
        )
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p1.pk]
        )
        self.assertEqual(
            list(self.nodes.p1.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p7.pk, self.nodes.p6.pk,
                self.nodes.p9.pk, self.nodes.p5.pk]
        )

    def test_can_move_a_node_after_a_sibling(self):
        self.nodes.p2.add_child(self.nodes.p9, sequence=12)
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p2,
            destination_sibling=self.nodes.p7,
            position=Position.AFTER
        )
        self.assertEqual(
            list(self.nodes.p2.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p5.pk, self.nodes.p7.pk,
                self.nodes.p9.pk, self.nodes.p6.pk]
        )

    def test_can_move_a_node_after_a_sibling_quickapi(self):
        self.nodes.p2.add_child(self.nodes.p9, sequence=12)
        self.nodes.p2.move_child_after(
            self.nodes.p9,
            self.nodes.p7,
        )
        self.assertEqual(
            list(self.nodes.p2.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p5.pk, self.nodes.p7.pk,
                self.nodes.p9.pk, self.nodes.p6.pk]
        )

    def test_can_move_a_node_between_parents_beforestart(self):
        self.nodes.p2.add_child(self.nodes.p9, sequence=12)
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p2.pk]
        )
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p1,
            destination_sibling=self.nodes.p7,
            position=Position.BEFORE
        )
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p1.pk]
        )
        self.assertEqual(
            list(self.nodes.p1.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p9.pk, self.nodes.p7.pk,
                self.nodes.p6.pk, self.nodes.p5.pk]
        )

    def test_can_move_a_node_between_parents_afterend(self):
        self.nodes.p2.add_child(self.nodes.p9, sequence=12)
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p2.pk]
        )
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p1,
            destination_sibling=self.nodes.p5,
            position=Position.AFTER
        )
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p1.pk]
        )
        self.assertEqual(
            list(self.nodes.p1.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p7.pk, self.nodes.p6.pk,
                self.nodes.p5.pk, self.nodes.p9.pk]
        )

    def test_queryset_sortting_with_nosep(self):
        for i in range(10, 16):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p6.insert_child_after(self.nodes.p10, None)
        self.nodes.p6.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p2.remove_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p3, sequence=9)
        self.nodes.p3.add_child(self.nodes.p13, sequence=6)

        with self.subTest(msg="with no cloned nodes"):
            qs = EdgeOrderedNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                    padsize=3,
                    padchar='0',
                    sepchar=''
            ).order_by('dag_node_path')
            self.assertEqual(
                tuple(
                    qs_sorted.values_list(
                        'pk',
                        'dag_sequence_path',
                        'dag_node_path',
                    )
                ),
                (
                    (1, '001', '0001'),
                    (5, '001012', '0001,0005'),
                    (6, '001008', '0001,0006'),
                    (10, '001008050', '0001,0006,0010'),
                    (12, '001008050050', '0001,0006,0010,0012'),
                    (11, '001008075', '0001,0006,0011'),
                    (7, '001004', '0001,0007'),
                    (2, '002', '0002'),
                    (3, '002009', '0002,0003'),
                    (13, '002009006', '0002,0003,0013'),
                    (5, '002001', '0002,0005'),
                    (7, '002004', '0002,0007'),
                    (4, '004', '0004'),
                    (8, '008', '0008'),
                    (9, '009', '0009'),
                    (14, '014', '0014'),
                    (15, '015', '0015')
                )
            )

    def test_queryset_sortting_with_no_padding(self):
        for i in range(10, 16):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p6.insert_child_after(self.nodes.p10, None)
        self.nodes.p6.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p2.remove_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p3, sequence=9)
        self.nodes.p3.add_child(self.nodes.p13, sequence=6)

        with self.subTest(msg="with no cloned nodes"):
            qs = EdgeOrderedNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                    padsize=0,
                    padchar='0',
                    sepchar=','
            ).order_by('dag_node_path')
            self.assertEqual(
                tuple(
                    qs_sorted.values_list(
                        'pk',
                        'dag_sequence_path',
                        'dag_node_path',
                    )
                ),
                (
                    (1, '1', '0001'),
                    (5, '1,12', '0001,0005'),
                    (6, '1,8', '0001,0006'),
                    (10, '1,8,50', '0001,0006,0010'),
                    (12, '1,8,50,50', '0001,0006,0010,0012'),
                    (11, '1,8,75', '0001,0006,0011'),
                    (7, '1,4', '0001,0007'),
                    (2, '2', '0002'),
                    (3, '2,9', '0002,0003'),
                    (13, '2,9,6', '0002,0003,0013'),
                    (5, '2,1', '0002,0005'),
                    (7, '2,4', '0002,0007'),
                    (4, '4', '0004'),
                    (8, '8', '0008'),
                    (9, '9', '0009'),
                    (14, '14', '0014'),
                    (15, '15', '0015')
                )
            )

    def test_queryset_sortting_with_neg_padding(self):
        for i in range(10, 16):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p6.insert_child_after(self.nodes.p10, None)
        self.nodes.p6.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p2.remove_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p3, sequence=9)
        self.nodes.p3.add_child(self.nodes.p13, sequence=6)

        with self.subTest(msg="with no cloned nodes"):
            qs = EdgeOrderedNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                    padsize=-3,
                    padchar='0',
                    sepchar=','
            ).order_by('dag_node_path')
            self.assertEqual(
                tuple(
                    qs_sorted.values_list(
                        'pk',
                        'dag_sequence_path',
                        'dag_node_path',
                    )
                ),
                (
                    (1, '100', '0001'),
                    (5, '100,120', '0001,0005'),
                    (6, '100,800', '0001,0006'),
                    (10, '100,800,500', '0001,0006,0010'),
                    (12, '100,800,500,500', '0001,0006,0010,0012'),
                    (11, '100,800,750', '0001,0006,0011'),
                    (7, '100,400', '0001,0007'),
                    (2, '200', '0002'),
                    (3, '200,900', '0002,0003'),
                    (13, '200,900,600', '0002,0003,0013'),
                    (5, '200,100', '0002,0005'),
                    (7, '200,400', '0002,0007'),
                    (4, '400', '0004'),
                    (8, '800', '0008'),
                    (9, '900', '0009'),
                    (14, '140', '0014'),
                    (15, '150', '0015')
                )
            )

    def test_queryset_sortting_with_custom_query(self):
        for i in range(10, 16):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p6.insert_child_after(self.nodes.p10, None)
        self.nodes.p6.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p2.remove_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p3, sequence=9)
        self.nodes.p3.add_child(self.nodes.p13, sequence=6)

        class CustomQuerySet(EdgeOrderedNode.objects._queryset_class):
            path_padding_size = 2
            path_padding_char = ' '
            path_seperator = '+'

        class TestSortableCSModel(EdgeOrderedNode):
            class Meta:
                proxy = True
            objects = EdgeOrderedNode._default_manager.from_queryset(CustomQuerySet)()

        with self.subTest(msg="with no cloned nodes"):
            qs = TestSortableCSModel.objects.all()
            qs_sorted = qs.with_sort_sequence(
                    padsize=3,
                    padchar='0',
                    sepchar=','
            ).order_by('dag_node_path')
            self.assertEqual(
                tuple(
                    qs_sorted.values_list(
                        'pk',
                        'dag_sequence_path',
                        'dag_node_path',
                    )
                ),
                (
                    (1, '001', ' 1'),
                    (5, '001,012', ' 1+ 5'),
                    (6, '001,008', ' 1+ 6'),
                    (10, '001,008,050', ' 1+ 6+10'),
                    (12, '001,008,050,050', ' 1+ 6+10+12'),
                    (11, '001,008,075', ' 1+ 6+11'),
                    (7, '001,004', ' 1+ 7'),
                    (2, '002', ' 2'),
                    (3, '002,009', ' 2+ 3'),
                    (13, '002,009,006', ' 2+ 3+13'),
                    (5, '002,001', ' 2+ 5'),
                    (7, '002,004', ' 2+ 7'),
                    (4, '004', ' 4'),
                    (8, '008', ' 8'),
                    (9, '009', ' 9'),
                    (14, '014', '14'),
                    (15, '015', '15')
                )
            )

    def test_queryset_sortting_with_mixed_different_settings(self):
        for i in range(10, 16):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p6.insert_child_after(self.nodes.p10, None)
        self.nodes.p6.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p2.remove_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p3, sequence=9)
        self.nodes.p3.add_child(self.nodes.p13, sequence=6)

        with self.subTest(msg="with no cloned nodes"):
            qs = EdgeOrderedNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                DagSortOrder.NODE_PK,
                padsize=2,
                padchar='-',
                sepchar='+',
            )
            qs_sorted = qs_sorted.with_sort_sequence(
                DagSortOrder.NODE_SEQUENCE,
                padsize=3,
                padchar='0',
                sepchar='-',
            ).order_by('dag_pk_path')
            self.assertEqual(
                tuple(
                    qs_sorted.values_list(
                        'pk',
                        'dag_sequence_path',
                        'dag_pk_path',
                        'dag_node_path'
                    )
                ),
                (
                    (1, '001', '-1', '0001'),
                    (5, '001-012', '-1+-5', '0001,0005'),
                    (6, '001-008', '-1+-6', '0001,0006'),
                    (10, '001-008-050', '-1+-6+10', '0001,0006,0010'),
                    (12, '001-008-050-050', '-1+-6+10+12', '0001,0006,0010,0012'),
                    (11, '001-008-075', '-1+-6+11', '0001,0006,0011'),
                    (7, '001-004', '-1+-7', '0001,0007'),
                    (2, '002', '-2', '0002'),
                    (3, '002-009', '-2+-3', '0002,0003'),
                    (13, '002-009-006', '-2+-3+13', '0002,0003,0013'),
                    (5, '002-001', '-2+-5', '0002,0005'),
                    (7, '002-004', '-2+-7', '0002,0007'),
                    (4, '004', '-4', '0004'),
                    (8, '008', '-8', '0008'),
                    (9, '009', '-9', '0009'),
                    (14, '014', '14', '0014'),
                    (15, '015', '15', '0015')
                )
            )

    def test_queryset_sortting_with_double_used_different_settings(self):
        for i in range(10, 16):
            n = EdgeOrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p6.insert_child_after(self.nodes.p10, None)
        self.nodes.p6.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p2.remove_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p3, sequence=9)
        self.nodes.p3.add_child(self.nodes.p13, sequence=6)

        with self.subTest(msg="with no cloned nodes"):
            qs = EdgeOrderedNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                DagSortOrder.NODE_SEQUENCE,
                padsize=2,
                padchar='-',
                sepchar='+',
                name='custom_path'
            )
            qs_sorted = qs_sorted.with_sort_sequence(
                DagSortOrder.NODE_SEQUENCE,
                padsize=3,
                padchar='0',
                sepchar='-',
            ).order_by('dag_node_path')
            self.assertEqual(
                tuple(
                    qs_sorted.values_list(
                        'pk',
                        'dag_sequence_path',
                        'custom_path',
                        'dag_node_path'
                    )
                ),
                (
                    (1, '001', '-1', '0001'),
                    (5, '001-012', '-1+12', '0001,0005'),
                    (6, '001-008', '-1+-8', '0001,0006'),
                    (10, '001-008-050', '-1+-8+50', '0001,0006,0010'),
                    (12, '001-008-050-050', '-1+-8+50+50', '0001,0006,0010,0012'),
                    (11, '001-008-075', '-1+-8+75', '0001,0006,0011'),
                    (7, '001-004', '-1+-4', '0001,0007'),
                    (2, '002', '-2', '0002'),
                    (3, '002-009', '-2+-9', '0002,0003'),
                    (13, '002-009-006', '-2+-9+-6', '0002,0003,0013'),
                    (5, '002-001', '-2+-1', '0002,0005'),
                    (7, '002-004', '-2+-4', '0002,0007'),
                    (4, '004', '-4', '0004'),
                    (8, '008', '-8', '0008'),
                    (9, '009', '-9', '0009'),
                    (14, '014', '14', '0014'),
                    (15, '015', '15', '0015')
                )
            )


class NodeSortedDagRelationshipTests(TestCase):
    def setUp(self):
        self.nodes = NodeStorage()
        for i in range(1, 10):
            n = OrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        # `-- <BasicNode: # 1>
        #     `-- <BasicNode: # 5 o=1 go=2 >
        #     `-- <BasicNode: # 4 o=2 go=6 >
        #     `-- <BasicNode: # 3 o=3 go=12 >
        # `-- <BasicNode: # 2>
        #     `-- <BasicNode: # 6 o=1 go=1 >
        #     `-- <BasicNode: # 8 o=2 go=8 >
        #     `-- <BasicNode: # 7 o=3 go=11 >
        self.nodes.p1.add_child(self.nodes.p3)
        self.nodes.p1.add_child(self.nodes.p4)
        self.nodes.p1.add_child(self.nodes.p5)
        self.nodes.p2.add_child(self.nodes.p6)
        self.nodes.p2.add_child(self.nodes.p7)
        self.nodes.p2.add_child(self.nodes.p8)

        self.nodes.p3.sequence = 12
        self.nodes.p4.sequence = 6
        self.nodes.p5.sequence = 2
        self.nodes.p6.sequence = 1
        self.nodes.p7.sequence = 11
        self.nodes.p8.sequence = 8
        for k, n in self.nodes.__dict__.items():
            if k.startswith('p'):
                n.save()

    def test_can_add_and_set_sequence(self):
        self.nodes.p1.add_child(self.nodes.p9, sequence=7)
        self.assertEqual(
            list(self.nodes.p1.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p5.pk, self.nodes.p4.pk,
                self.nodes.p9.pk, self.nodes.p3.pk]
        )

    def test_queryset_sortting_filter(self):
        for i in range(10, 16):
            n = OrderedNode(name="%s" % i)
            n.save()
            setattr(self.nodes, "p%s" % i, n)
        self.nodes.p4.insert_child_after(self.nodes.p10, None)
        self.nodes.p4.insert_child_after(self.nodes.p11, self.nodes.p10)
        self.nodes.p10.insert_child_after(self.nodes.p12, None)
        self.nodes.p7.insert_child_after(self.nodes.p13, None)

        with self.subTest(msg="with no cloned nodes"):
            qs = OrderedNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                DagSortOrder.NODE_SEQUENCE,
                padsize=2,
            ).order_by('dag_sequence_path')
            self.assertEqual(
                tuple(qs_sorted.values_list('pk', 'dag_sequence_path')),
                (
                    (1, '01'),
                    (5, '01,02'),
                    (4, '01,06'),
                    (10, '01,06,50'),
                    (12, '01,06,50,50'),
                    (11, '01,06,75'),
                    (3, '01,12'),
                    (2, '02'),
                    (6, '02,01'),
                    (8, '02,08'),
                    (7, '02,11'),
                    (13, '02,11,50'),
                    (9, '09'),
                    (14, '14'),
                    (15, '15')
                )
            )
        with self.subTest(msg="with cloned nodes"):
            self.nodes.p6.insert_child_after(self.nodes.p10, None)
            qs = OrderedNode.objects.all()
            qs_sorted = qs.with_sort_sequence(
                DagSortOrder.NODE_SEQUENCE,
                padsize=2,
            ).order_by('dag_sequence_path')
            self.assertEqual(
                tuple(qs_sorted.values_list('pk', 'dag_sequence_path')),
                (
                    (1, '01'),
                    (5, '01,02'),
                    (4, '01,06'),
                    (10, '01,06,50'),
                    (12, '01,06,50,50'),
                    (11, '01,06,75'),
                    (3, '01,12'),
                    (2, '02'),
                    (6, '02,01'),
                    (10, '02,01,50'),
                    (12, '02,01,50,50'),
                    (8, '02,08'),
                    (7, '02,11'),
                    (13, '02,11,50'),
                    (9, '09'),
                    (14, '14'),
                    (15, '15')
                )
            )

    def test_children_ordered_filter(self):
        self.assertEqual(
            list(self.nodes.p1.children
                 .with_sequence().order_by('sequence')
                 .values_list('name', 'sequence')),
            [('5', 2), ('4', 6), ('3', 12)])
        self.assertEqual(
            list(self.nodes.p2.children
                 .with_sequence().order_by('sequence')
                 .values_list('name', 'sequence')),
            [('6', 1), ('8', 8), ('7', 11)])

    def test_ordered_filter_on_node(self):
        self.assertEqual(
            list(OrderedNode.objects
                 .with_sequence().order_by('sequence')
                 .filter(sequence__gt=0).values_list('name', 'sequence')),
            [('6', 1), ('5', 2), ('4', 6), ('8', 8), ('7', 11), ('3', 12)])

    def test_can_get_first_child_of_node(self):
        self.assertEqual(self.nodes.p1.get_first_child(), self.nodes.p5)
        self.assertEqual(self.nodes.p2.get_first_child(), self.nodes.p6)

    def test_can_get_last_child_of_node(self):
        self.assertEqual(self.nodes.p1.get_last_child(), self.nodes.p3)
        self.assertEqual(self.nodes.p2.get_last_child(), self.nodes.p7)

    def test_can_get_first_parent_of_node(self):
        self.nodes.p1.sequence = 10
        self.nodes.p9.sequence = 1
        self.nodes.p1.save()
        self.nodes.p9.save()

        self.nodes.p9.add_child(self.nodes.p4)
        self.assertEqual(self.nodes.p4.get_first_parent(), self.nodes.p9)
        self.nodes.p9.sequence = 20
        self.nodes.p9.save()
        self.assertEqual(self.nodes.p4.get_first_parent(), self.nodes.p1)
        self.assertEqual(
            self.nodes.p4.get_first_parent(),
            self.nodes.p4.parents.with_sequence().order_by('sequence').first(),
        )

    def test_can_get_last_parent_of_node(self):
        self.nodes.p1.sequence = 10
        self.nodes.p9.sequence = 1
        self.nodes.p1.save()
        self.nodes.p9.save()

        self.nodes.p9.add_child(self.nodes.p4)
        self.assertEqual(self.nodes.p4.get_last_parent(), self.nodes.p1)
        self.nodes.p9.sequence = 20
        self.nodes.p9.save()
        self.assertEqual(self.nodes.p4.get_last_parent(), self.nodes.p9)
        self.assertEqual(
            self.nodes.p4.get_last_parent(),
            self.nodes.p4.parents.with_sequence().order_by('sequence').last(),
        )

    def test_can_get_next_sibling_of_node(self):
        self.assertEqual(
            self.nodes.p5.get_next_sibling(self.nodes.p1),
            self.nodes.p4)
        self.assertEqual(self.nodes.p3.get_next_sibling(self.nodes.p1), None)
        self.assertEqual(
            self.nodes.p6.get_next_sibling(self.nodes.p2),
            self.nodes.p8)
        self.assertEqual(
            self.nodes.p7.get_next_sibling(self.nodes.p2), None)

    def test_can_get_prev_sibling_of_node(self):
        self.assertEqual(self.nodes.p5.get_prev_sibling(self.nodes.p1), None)
        self.assertEqual(
            self.nodes.p4.get_prev_sibling(self.nodes.p1),
            self.nodes.p5)
        self.assertEqual(
            self.nodes.p8.get_prev_sibling(self.nodes.p2),
            self.nodes.p6)
        self.assertEqual(
            self.nodes.p6.get_prev_sibling(self.nodes.p2), None)

    def test_cannot_move_a_node_between_parents_causing_circular_ref(self):
        self.nodes.p3.add_child(self.nodes.p9)
        self.nodes.p9.sequence = 12
        self.nodes.p9.save()
        with self.assertRaises(InvalidNodeMove):
            self.nodes.p1.move_node(
                None,
                self.nodes.p9,
            )

    def test_can_move_a_node_between_parents_default_location(self):
        self.nodes.p2.add_child(self.nodes.p9)
        self.nodes.p9.sequence = 12
        self.nodes.p9.save()
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p2.pk]
        )
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p1,
        )
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p1.pk]
        )

    def test_can_move_a_node_between_parents_first(self):
        self.nodes.p2.add_child(self.nodes.p9)
        self.nodes.p9.sequence = 12
        self.nodes.p9.save()

        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p2.pk]
        )
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p1,
            position=Position.FIRST
        )
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p1.pk]
        )
        self.assertEqual(
            self.nodes.p1.children
                .with_sequence().order_by('sequence')
                .values_list('pk', flat=True).first(),
            self.nodes.p9.pk
        )

    def test_can_move_a_sibling_node_to_be_first(self):
        self.nodes.p1.add_child(self.nodes.p9)
        self.nodes.p9.sequence = 14
        self.nodes.p9.save()

        self.nodes.p9.move_node(
            self.nodes.p1,
            self.nodes.p1,
            position=Position.FIRST
        )
        self.assertEqual(
            self.nodes.p1.children
                .with_sequence().order_by('sequence')
                .values_list('pk', flat=True).first(),
            self.nodes.p9.pk
        )

    def test_can_move_a_root_node_to_be_firstchild_of_another_node(self):
        self.nodes.p9.move_node(
            None,
            self.nodes.p1,
            position=Position.FIRST
        )
        self.assertEqual(
            self.nodes.p1.children
                .with_sequence().order_by('sequence')
                .values_list('pk', flat=True).first(),
            self.nodes.p9.pk
        )

    def test_can_move_a_node_between_parents_last(self):
        self.nodes.p2.add_child(self.nodes.p9)
        self.nodes.p9.sequence = 12
        self.nodes.p9.save()

        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p2.pk]
        )
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p1,
            position=Position.LAST
        )
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p1.pk]
        )
        self.assertEqual(
            self.nodes.p1.children
                .with_sequence().order_by('sequence')
                .values_list('pk', flat=True).last(),
            self.nodes.p9.pk
        )

    def test_can_move_a_sibling_node_to_be_last(self):
        self.nodes.p1.add_child(self.nodes.p9)
        self.nodes.p9.sequence = 2
        self.nodes.p9.save()

        self.nodes.p9.move_node(
            self.nodes.p1,
            self.nodes.p1,
            position=Position.LAST
        )
        self.assertEqual(
            self.nodes.p1.children
                .with_sequence().order_by('sequence')
                .values_list('pk', flat=True).last(),
            self.nodes.p9.pk
        )

    def test_can_move_a_root_node_to_be_lastchild_of_another_node(self):
        self.nodes.p9.move_node(
            None,
            self.nodes.p1,
            position=Position.LAST
        )
        self.assertEqual(
            self.nodes.p1.children
                .with_sequence().order_by('sequence')
                .values_list('pk', flat=True).last(),
            self.nodes.p9.pk
        )

    def test_can_move_a_node_between_parents_before(self):
        self.nodes.p2.add_child(self.nodes.p9)
        self.nodes.p9.sequence = 12
        self.nodes.p9.save()

        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p2.pk]
        )
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p1,
            destination_sibling=self.nodes.p4,
            position=Position.BEFORE
        )
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p1.pk]
        )
        self.assertEqual(
            list(self.nodes.p1.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p5.pk, self.nodes.p9.pk,
                self.nodes.p4.pk, self.nodes.p3.pk]
        )

    def test_can_move_a_node_before_a_sibling(self):
        self.nodes.p2.add_child(self.nodes.p9)
        self.nodes.p9.sequence = 12
        self.nodes.p9.save()

        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p2,
            destination_sibling=self.nodes.p7,
            position=Position.BEFORE
        )
        self.assertEqual(
            list(self.nodes.p2.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p6.pk, self.nodes.p8.pk,
                self.nodes.p9.pk, self.nodes.p7.pk]
        )

    def test_can_move_a_node_before_a_sibling_quickapi(self):
        self.nodes.p2.add_child(self.nodes.p9)
        self.nodes.p9.sequence = 12
        self.nodes.p9.save()

        self.nodes.p2.move_child_before(
            self.nodes.p9,
            self.nodes.p7,
        )
        self.assertEqual(
            list(self.nodes.p2.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p6.pk, self.nodes.p8.pk,
                self.nodes.p9.pk, self.nodes.p7.pk]
        )

    def test_can_move_a_node_between_parents_after(self):
        self.nodes.p2.add_child(self.nodes.p9)
        self.nodes.p9.sequence = 12
        self.nodes.p9.save()

        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p2.pk]
        )
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p1,
            destination_sibling=self.nodes.p4,
            position=Position.AFTER
        )
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p1.pk]
        )
        self.assertEqual(
            list(self.nodes.p1.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p5.pk, self.nodes.p4.pk,
                self.nodes.p9.pk, self.nodes.p3.pk]
        )

    def test_can_move_a_node_after_a_sibling(self):
        self.nodes.p2.add_child(self.nodes.p9)
        self.nodes.p9.sequence = 12
        self.nodes.p9.save()

        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p2,
            destination_sibling=self.nodes.p8,
            position=Position.AFTER
        )
        self.assertEqual(
            list(self.nodes.p2.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p6.pk, self.nodes.p8.pk,
                self.nodes.p9.pk, self.nodes.p7.pk]
        )

    def test_can_move_a_node_after_a_sibling_quickapi(self):
        self.nodes.p2.add_child(self.nodes.p9)
        self.nodes.p9.sequence = 12
        self.nodes.p9.save()

        self.nodes.p2.move_child_after(
            self.nodes.p9,
            self.nodes.p8,
        )
        self.assertEqual(
            list(self.nodes.p2.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p6.pk, self.nodes.p8.pk,
                self.nodes.p9.pk, self.nodes.p7.pk]
        )

    def test_can_move_a_node_between_parents_beforestart(self):
        self.nodes.p2.add_child(self.nodes.p9)
        self.nodes.p9.sequence = 12
        self.nodes.p9.save()

        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p2.pk]
        )
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p1,
            destination_sibling=self.nodes.p5,
            position=Position.BEFORE
        )
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p1.pk]
        )
        self.assertEqual(
            list(self.nodes.p1.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p9.pk, self.nodes.p5.pk,
                self.nodes.p4.pk, self.nodes.p3.pk]
        )

    def test_can_move_a_node_between_parents_afterend(self):
        self.nodes.p2.add_child(self.nodes.p9)
        self.nodes.p9.sequence = 12
        self.nodes.p9.save()

        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p2.pk]
        )
        self.nodes.p9.move_node(
            self.nodes.p2,
            self.nodes.p1,
            destination_sibling=self.nodes.p3,
            position=Position.AFTER
        )
        self.assertEqual(
            list(self.nodes.p9.parents.values_list('pk', flat=True)),
            [self.nodes.p1.pk]
        )
        self.assertEqual(
            list(self.nodes.p1.children
                 .with_sequence().order_by('sequence')
                 .values_list('pk', flat=True)),
            [self.nodes.p5.pk, self.nodes.p4.pk,
                self.nodes.p3.pk, self.nodes.p9.pk]
        )
