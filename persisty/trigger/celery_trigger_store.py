from dataclasses import dataclass
from typing import Optional, Iterator

from marshy.types import ExternalItemType
from servey.action.action import Action
from servey.finder.action_finder_abc import find_actions_with_trigger_type
from servey.servey_celery import celery_app

from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC, T
from persisty.trigger.store_triggers import StoreTriggers


@dataclass
class CeleryTriggerStore(WrapperStoreABC[T]):
    """
    Store which triggers actions after edits using celery
    """

    store: StoreABC
    store_triggers: StoreTriggers

    def get_store(self) -> StoreABC:
        return self.store

    def create(self, item: T) -> Optional[T]:
        result = self.store.create(item)
        if result:
            for action_ in self.store_triggers.get_after_create_actions():
                task = getattr(celery_app, action_.name)
                task.apply_async(item)
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
            for action_ in self.store_triggers.get_after_update_actions():
                task = getattr(celery_app, action_.name)
                task.apply_async(item, new_item)
            return new_item

    def delete(self, key: str) -> bool:
        if not self.store_triggers.has_after_delete_actions():
            return self.store.delete(key)
        return StoreABC.delete(self, key)

    def _delete(self, key: str, item: T) -> bool:
        # pylint: disable=W0212
        result = self.store._delete(key, item)
        if result:
            for action_ in self.store_triggers.get_after_delete_actions():
                task = getattr(celery_app, action_.name)
                task.apply_async(item)
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


def _get_triggered_actions(storage_name: str, trigger_type) -> Iterator[Action]:
    for action, trigger in find_actions_with_trigger_type(trigger_type):
        if trigger.storage_name == storage_name:
            yield action
