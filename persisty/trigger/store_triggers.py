import inspect
from dataclasses import dataclass, field
from typing import List, Iterator, Awaitable

from marshy import get_default_context
from marshy.marshaller_context import MarshallerContext
from servey.action.action import Action
from servey.finder.action_finder_abc import find_actions_with_trigger_type

from persisty.store_meta import StoreMeta
from persisty.trigger.after_create_trigger import AfterCreateTrigger
from persisty.trigger.after_delete_trigger import AfterDeleteTrigger
from persisty.trigger.after_update_trigger import AfterUpdateTrigger


@dataclass
class StoreTriggers:
    """
    Object for collecting / coordingating triggers to be run after edits to a store object.
    The actual mechanism for running triggers may be asyncio, celery, or even a post processing
    lambda on a dynamodb table.
    """

    store_meta: StoreMeta
    after_create_actions: List[Action] = None
    after_update_actions: List[Action] = None
    after_delete_actions: List[Action] = None
    marshaller_context: MarshallerContext = field(default_factory=get_default_context)

    def get_after_create_actions(self):
        if self.after_create_actions is None:
            self.after_create_actions = list(
                _get_triggered_actions(self.store_meta.name, AfterCreateTrigger)
            )
        return self.after_create_actions

    def get_after_update_actions(self):
        if self.after_create_actions is None:
            self.after_create_actions = list(
                _get_triggered_actions(self.store_meta.name, AfterUpdateTrigger)
            )
        return self.after_create_actions

    def get_after_delete_actions(self):
        if self.after_create_actions is None:
            self.after_create_actions = list(
                _get_triggered_actions(self.store_meta.name, AfterDeleteTrigger)
            )
        return self.after_create_actions

    def has_after_create_actions(self):
        return bool(self.get_after_create_actions())

    def has_after_update_actions(self):
        return bool(self.get_after_update_actions())

    def has_after_delete_actions(self):
        return bool(self.get_after_delete_actions())

    def has_after_edit_actions(self):
        return (
            self.has_after_create_actions()
            or self.has_after_update_actions()
            or self.has_after_delete_actions()
        )

    async def async_after_create(self, new_item):
        for action in self.get_after_create_actions():
            result = action.fn(new_item)
            if isinstance(result, Awaitable):
                await result

    async def async_after_update(self, old_item, new_item):
        for action in self.get_after_create_actions():
            result = action.fn(old_item, new_item)
            if isinstance(result, Awaitable):
                await result

    async def async_after_delete(self, old_item):
        for action in self.get_after_create_actions():
            result = action.fn(old_item)
            if isinstance(result, Awaitable):
                await result


def _get_triggered_actions(store_name: str, trigger_type) -> Iterator[Action]:
    for action, trigger in find_actions_with_trigger_type(trigger_type):
        if trigger.store_name == store_name:
            yield action
