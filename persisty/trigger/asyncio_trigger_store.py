import asyncio
from dataclasses import dataclass
from typing import Optional, Iterator

from marshy.types import ExternalItemType
from servey.action.action import Action
from servey.finder.action_finder_abc import find_actions_with_trigger_type

from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC
from persisty.trigger.store_triggers import StoreTriggers


@dataclass
class AsyncioTriggerStore(WrapperStoreABC):
    """
    Store which runs triggers after edits using asyncio
    """

    store: StoreABC
    store_triggers: StoreTriggers

    def get_store(self) -> StoreABC:
        return self.store

    def create(self, item: ExternalItemType) -> Optional[ExternalItemType]:
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
        key = self.get_meta().key_config.to_key_str(updates)
        old_item = self.store.read(key)
        if old_item:
            new_item = self.store.update(updates, precondition)
            if new_item:
                coro = self.store_triggers.async_after_update(old_item, new_item)
                asyncio.ensure_future(coro)
                return new_item

    def delete(self, key: str) -> bool:
        if not self.store_triggers.has_after_delete_actions():
            return self.store.delete(key)
        result = False
        item = self.store.read(key)
        if item:
            result = self.store.delete(key)
            if result:
                coro = self.store_triggers.async_after_delete(item)
                asyncio.ensure_future(coro)
        return result


def _get_triggered_actions(store_name: str, trigger_type) -> Iterator[Action]:
    for action, trigger in find_actions_with_trigger_type(trigger_type):
        if trigger.store_name == store_name:
            yield action
