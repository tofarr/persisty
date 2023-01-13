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
        key = self.get_meta().key_config.to_key_str(updates)
        old_item = self.store.read(key)
        if old_item:
            new_item = self.store.update(updates, precondition)
            if new_item:
                for action_ in self.store_triggers.get_after_create_actions():
                    task = getattr(celery_app, action_.name)
                    task.apply_async(old_item, new_item)
                return new_item

    def delete(self, key: str) -> bool:
        if not self.store_triggers.has_after_delete_actions():
            return self.store.delete(key)
        result = False
        item = self.store.read(key)
        if item:
            result = self.store.delete(key)
            if result:
                for action_ in self.store_triggers.get_after_create_actions():
                    task = getattr(celery_app, action_.name)
                    task.apply_async(item)
        return result


def _get_triggered_actions(storage_name: str, trigger_type) -> Iterator[Action]:
    for action, trigger in find_actions_with_trigger_type(trigger_type):
        if trigger.storage_name == storage_name:
            yield action
