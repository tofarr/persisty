from dataclasses import dataclass
from typing import Optional, Iterator

from marshy.types import ExternalItemType
from servey.action.action import Action
from servey.finder.action_finder_abc import find_actions_with_trigger_type
from servey.servey_celery import celery_app

from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.storage_abc import StorageABC
from persisty.storage.wrapper_storage_abc import WrapperStorageABC
from persisty.trigger.storage_triggers import StorageTriggers


@dataclass
class CeleryTriggerStorage(WrapperStorageABC):
    """
    Storage which triggers actions after edits using celery
    """

    storage: StorageABC
    storage_triggers: StorageTriggers

    def get_storage(self) -> StorageABC:
        return self.storage

    def create(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        result = self.storage.create(item)
        if result:
            for action_ in self.storage_triggers.get_after_create_actions():
                loaded = self.storage_triggers.load_item(action_, result)
                task = getattr(celery_app, action_.name)
                task.apply_async(loaded)
            return result

    def update(
        self, updates: ExternalItemType, precondition: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[ExternalItemType]:
        if not self.storage_triggers.has_after_update_actions():
            return self.storage.update(updates, precondition)
        key = self.get_storage_meta().key_config.to_key_str(updates)
        old_item = self.storage.read(key)
        if old_item:
            new_item = self.storage.update(updates, precondition)
            if new_item:
                for action_ in self.storage_triggers.get_after_create_actions():
                    loaded_new = self.storage_triggers.load_item(action_, new_item)
                    loaded_old = self.storage_triggers.load_item(action_, old_item)
                    task = getattr(celery_app, action_.name)
                    task.apply_async(loaded_old, loaded_new)
                return new_item

    def delete(self, key: str) -> bool:
        if not self.storage_triggers.has_after_delete_actions():
            return self.storage.delete(key)
        result = False
        item = self.storage.read(key)
        if item:
            result = self.storage.delete(key)
            if result:
                for action_ in self.storage_triggers.get_after_create_actions():
                    loaded = self.storage_triggers.load_item(action_, item)
                    task = getattr(celery_app, action_.name)
                    task.apply_async(loaded)
        return result


def _get_triggered_actions(storage_name: str, trigger_type) -> Iterator[Action]:
    for action, trigger in find_actions_with_trigger_type(trigger_type):
        if trigger.storage_name == storage_name:
            yield action
