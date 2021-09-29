from django_delayed_union import DelayedUnionQuerySet
from django_delayed_union.base import NotImplementedMethod


class DagBaseDelayedUnionQuerySet(DelayedUnionQuerySet):
    with_sort_sequence = NotImplementedMethod()
    distinct_node = NotImplementedMethod()
