from dataclasses import field, dataclass
from typing import Optional, ForwardRef
from unittest import TestCase
from uuid import uuid4

from old.persisty.persisty_context import get_default_persisty_context
from persisty.errors import PersistyError
from persisty.obj_graph import EntityABC
from persisty.obj_graph import OnDestroy
from persisty.obj_graph import BelongsTo
from persisty.obj_graph import HasOne
from old.persisty.storage.in_mem_storage import in_mem_storage

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
        foo_storage = in_mem_storage(Foo)
        for foo in FOOS:
            foo_storage.create(foo)
        persisty_context.register_storage(foo_storage)
        bar_storage = in_mem_storage(Bar)
        for bar in BARS:
            bar_storage.create(bar)
        persisty_context.register_storage(bar_storage)

    def test_read_missing(self):
        empty = FooEntity('Missing', None)
        assert empty.bar is None
        empty = FooEntity('Missing')
        assert empty.bar is None

    def test_destroy_cascade(self):
        class CascadingFooEntity(EntityABC, Foo):
            bar: BAR_ENTITY = HasOne('foo_id', on_destroy=OnDestroy.CASCADE)
        self._do_destroy(CascadingFooEntity)
        storage = get_default_persisty_context().get_storage(Bar)
        assert storage.read('bar_1') is None

    @staticmethod
    def _do_destroy(entity):
        foo = entity.read('foo_1')
        assert foo.bar == BARS[0]
        foo.destroy()

    def test_destroy_nullify(self):
        class NullifyingFooEntity(EntityABC, Foo):
            bar: BAR_ENTITY = HasOne('foo_id', on_destroy=OnDestroy.NULLIFY)
        self._do_destroy(NullifyingFooEntity)
        bar = get_default_persisty_context().get_storage(Bar).read('bar_1')
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
