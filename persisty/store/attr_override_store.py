import dataclasses
from dataclasses import dataclass
from typing import Optional

from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store.filtered_store_abc import FilteredStoreABC, T
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta
from persisty.util import UNDEFINED


@dataclass(frozen=True)
class AttrOverrideStore(FilteredStoreABC[T]):
    store: StoreABC[T]
    attr_name: str
    creatable: bool = True
    readable: bool = True
    updatable: bool = True
    create_generator: Optional[AttrValueGeneratorABC] = None
    update_generator: Optional[AttrValueGeneratorABC] = None

    def __post_init__(self):
        meta = self.store.get_meta()
        attrs = []
        for attr in meta.attrs:
            if attr.name == self.attr_name:
                attr = dataclasses.replace(
                    attr,
                    creatable=self.creatable,
                    readable=self.readable,
                    updatable=self.updatable,
                    create_generator=self.create_generator or attr.create_generator,
                    update_generator=self.update_generator or attr.update_generator,
                    sortable=attr.sortable and self.readable,
                    permitted_filter_ops=attr.permitted_filter_ops if self.readable else tuple()
                )
            attrs.append(attr)
        meta = dataclasses.replace(meta, attrs=tuple(attrs))
        object.__setattr__(self, '_meta', meta)

    def get_meta(self) -> StoreMeta:
        return getattr(self, '_meta')

    def get_store(self) -> StoreABC:
        return self.store

    def filter_create(self, item: T) -> Optional[T]:
        kwargs = dataclasses.asdict(item)
        if self.create_generator:
            value = getattr(item, self.attr_name, UNDEFINED)
            value = self.create_generator.transform(value, item)
            kwargs[self.attr_name] = value
        result = self.store.get_meta().get_create_dataclass()(**kwargs)
        return result

    def filter_update(self, item: T, updates: T) -> T:
        kwargs = dataclasses.asdict(updates)
        if self.update_generator:
            value = getattr(updates, self.attr_name, UNDEFINED)
            if value is UNDEFINED:
                value = getattr(item, self.attr_name, UNDEFINED)
            value = self.update_generator.transform(value, item)
            kwargs[self.attr_name] = value
        result = self.store.get_meta().get_update_dataclass()(**kwargs)
        return result

    def update_all(self, search_filter: SearchFilterABC[T], updates: T):
        return self.store.update_all(search_filter, updates)

    def delete_all(self, search_filter: SearchFilterABC[T]):
        return self.store.delete_all(search_filter)
