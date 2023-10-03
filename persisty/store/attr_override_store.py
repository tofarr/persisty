import dataclasses
from dataclasses import dataclass
from typing import Optional

from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store.filtered_store_abc import FilteredStoreABC, T
from persisty.store.store_abc import StoreABC


@dataclass(frozen=True)
class AttrOverrideStore(FilteredStoreABC[T]):
    store: StoreABC[T]
    attr_name: str
    creatable: bool = True
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
                    create_generator=self.create_generator or attr.create_generator,
                    updatable=self.updatable,
                    update_generator=self.update_generator or attr.update_generator,
                )
            attrs.append(attr)

    def get_store(self) -> StoreABC:
        return self.store

    def filter_create(self, item: T) -> Optional[T]:
        if self.create_generator:
            value = getattr(item, self.attr_name)
            value = self.create_generator.transform(value, item)
            item = dataclasses.replace(item, **{self.attr_name: value})
        return item

    def filter_update(self, item: T, updates: T) -> T:
        if self.update_generator:
            value = getattr(item, self.attr_name)
            value = self.update_generator.transform(value, item)
            item = dataclasses.replace(item, **{self.attr_name: value})
        return item

    def update_all(self, search_filter: SearchFilterABC[T], updates: T):
        return self.store.update_all(search_filter, updates)

    def delete_all(self, search_filter: SearchFilterABC[T]):
        return self.store.delete_all(search_filter)
