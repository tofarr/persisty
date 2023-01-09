from servey.servey_celery import has_celery_broker

from persisty.storage.storage_abc import StorageABC
from persisty.trigger.storage_triggers import StorageTriggers
from persisty.trigger.asyncio_trigger_storage import AsyncioTriggerStorage


def triggered_storage(storage: StorageABC):
    storage_triggers = StorageTriggers(storage.get_storage_meta())
    if storage_triggers.has_after_edit_actions():
        if has_celery_broker():
            from persisty.trigger.celery_trigger_storage import CeleryTriggerStorage
            storage = CeleryTriggerStorage(storage, storage_triggers)
        else:
            storage = AsyncioTriggerStorage(storage, storage_triggers)
    return storage
