from servey.servey_celery import has_celery_broker

from persisty.store.store_abc import StoreABC
from persisty.trigger.store_triggers import StoreTriggers
from persisty.trigger.asyncio_trigger_store import AsyncioTriggerStore


def triggered_store(store: StoreABC):
    store_triggers = StoreTriggers(store.get_meta())
    if store_triggers.has_after_edit_actions():
        if has_celery_broker():
            from persisty.trigger.celery_trigger_store import CeleryTriggerStore

            store = CeleryTriggerStore(store, store_triggers)
        else:
            store = AsyncioTriggerStore(store, store_triggers)
    return store
