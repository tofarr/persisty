import asyncio
from dataclasses import dataclass
from typing import Optional, Iterator

from marshy.types import ExternalItemType
from servey.action.action import Action
from servey.finder.action_finder_abc import find_actions_with_trigger_type

from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC, T
from persisty.trigger.store_triggers import StoreTriggers


@dataclass
class AsyncioTriggerStore(WrapperStoreABC[T]):
    """
    Store which runs triggers after edits using asyncio
    """

    store: StoreABC
    store_triggers: StoreTriggers

    def get_store(self) -> StoreABC:
        return self.store

    def create(self, item: T) -> Optional[T]:
        result = self.store.create(item)
        if result:
            coro = self.store_triggers.async_after_create(result)
            asyncio.ensure_future(coro)
            return result

    def update(
        self, updates: ExternalItemType, precondition: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[ExternalItemType]:
        if not self.store_triggers.has_after_update_actions():
            return self.store.update(updates, precondition)
        return StoreABC.update(self, updates, precondition)

    def _update(self, key: str, item: T, updates: T) -> Optional[T]:
        # pylint: disable=W0212
        new_item = self.store._update(key, item, updates)
        if new_item:
            coro = self.store_triggers.async_after_update(item, new_item)
            asyncio.ensure_future(coro)
            return new_item

    def delete(self, key: str) -> bool:
        if not self.store_triggers.has_after_delete_actions():
            return self.store.delete(key)
        return StoreABC.delete(self, key)

    def _delete(self, key: str, item: T) -> bool:
        # pylint: disable=W0212
        result = self.store._delete(key, item)
        if result:
            coro = self.store_triggers.async_after_delete(item)
            asyncio.ensure_future(coro)
        return result

    def update_all(self, search_filter: SearchFilterABC[T], updates: T):
        if self.store_triggers.has_after_update_actions():
            StoreABC.update_all(self, search_filter, updates)
        else:
            self.store.update_all(search_filter, updates)

    def delete_all(self, search_filter: SearchFilterABC[T]):
        if self.store_triggers.has_after_delete_actions():
            StoreABC.delete_all(self, search_filter)
        else:
            self.store.delete_all(search_filter)


def _get_triggered_actions(store_name: str, trigger_type) -> Iterator[Action]:
    for action, trigger in find_actions_with_trigger_type(trigger_type):
        if trigger.store_name == store_name:
            yield action
