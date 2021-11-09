from dataclasses import field, dataclass
from typing import Optional, ForwardRef
from unittest import TestCase
from uuid import uuid4

from persisty.persisty_context import get_default_persisty_context
from persisty.errors import PersistyError
from persisty.obj_graph.entity_abc import EntityABC
from persisty.obj_graph.resolver.before_destroy import OnDestroy
from persisty.obj_graph.resolver.belongs_to import BelongsTo
from persisty.obj_graph.resolver.has_one import HasOne
from persisty.store.in_mem_store import in_mem_store

FOO_ENTITY = ForwardRef(f'{__name__}.FooEntity')
BAR_ENTITY = ForwardRef(f'{__name__}.BarEntity')


@dataclass
class Foo:
    title: Optional[str]
    id: str = field(default_factory=lambda: str(uuid4()))


@dataclass
class Bar:
    title: Optional[str]
    id: str = field(default_factory=lambda: str(uuid4()))
    foo_id: Optional[str] = None


class FooEntity(EntityABC[Foo], Foo):
    bar: BAR_ENTITY = HasOne('foo_id')


class BarEntity(EntityABC[Bar], Bar):
    foo: FOO_ENTITY = BelongsTo()


FOOS = [Foo(f'Foo {i}', f'foo_{i}') for i in range(1, 10)]
BARS = [Bar(f'Bar {i}', f'bar_{i}', f'foo_{i}') for i in range(1, 10)]


class TestHasOne(TestCase):

    def setUp(self):
        persisty_context = get_default_persisty_context()
        foo_store = in_mem_store(Foo)
        for foo in FOOS:
            foo_store.create(foo)
        persisty_context.register_store(foo_store)
        bar_store = in_mem_store(Bar)
        for bar in BARS:
            bar_store.create(bar)
        persisty_context.register_store(bar_store)

    def test_read_missing(self):
        empty = FooEntity('Missing', None)
        assert empty.bar is None
        empty = FooEntity('Missing')
        assert empty.bar is None

    def test_destroy_cascade(self):
        class CascadingFooEntity(EntityABC, Foo):
            bar: BAR_ENTITY = HasOne('foo_id', on_destroy=OnDestroy.CASCADE)
        self._do_destroy(CascadingFooEntity)
        store = get_default_persisty_context().get_store(Bar)
        assert store.read('bar_1') is None

    @staticmethod
    def _do_destroy(entity):
        foo = entity.read('foo_1')
        assert foo.bar == BARS[0]
        foo.destroy()

    def test_destroy_nullify(self):
        class NullifyingFooEntity(EntityABC, Foo):
            bar: BAR_ENTITY = HasOne('foo_id', on_destroy=OnDestroy.NULLIFY)
        self._do_destroy(NullifyingFooEntity)
        bar = get_default_persisty_context().get_store(Bar).read('bar_1')
        assert bar.foo_id is None

    def test_destroy_invalid(self):
        with self.assertRaises(PersistyError):
            class NullifyingFooEntity(EntityABC, Foo):
                bar: str = HasOne('foo_id', on_destroy=OnDestroy.NULLIFY)

            self._do_destroy(NullifyingFooEntity)

    def test_update_with_set(self):
        foo = FooEntity.read('foo_1')
        foo.bar = BarEntity('Bar 1 Updated')
        foo.save()
        assert foo.bar.id != 'bar_1'
        assert FooEntity.read('foo_1') == foo
        bar = BarEntity.read(foo.bar.id)
        assert foo.bar == bar
        assert foo.bar.title == 'Bar 1 Updated'
        assert BarEntity.read('bar_1') is None

    def test_create_with_set(self):
        foo = FooEntity('Foo Create With Set', 'foo_create_with_set')
        foo.bar = BarEntity('Bar Create With Set', 'bar_create_with_set')
        foo.save()
        assert FooEntity.read('foo_create_with_set') == foo
        bar = BarEntity.read('bar_create_with_set')
        assert foo.bar == bar
        assert bar.foo_id == foo.id

    def test_unresolve_all(self):
        foo = FooEntity.read('foo_1')
        foo.bar = BarEntity('Bar 1 Updated', 'bar_1')
        foo.unresolve_all()
        assert foo.bar == BarEntity('Bar 1', 'bar_1', 'foo_1')
