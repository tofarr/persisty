import inspect
from dataclasses import dataclass, attr
from typing import Optional, Type

from boto3.dynamodb.types import TypeDeserializer
from marshy import ExternalType, get_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.marshaller_context import MarshallerContext
from marshy.types import ExternalItemType
from servey.action.action import Action
from servey.servey_aws.event_handler.event_handler_abc import (
    EventHandlerABC,
    EventHandlerFactoryABC,
)

from persisty.trigger.after_create_trigger import AfterCreateTrigger
from persisty.trigger.after_delete_trigger import AfterDeleteTrigger
from persisty.trigger.after_update_trigger import AfterUpdateTrigger


@dataclass
class DynamodbPostProcessEventHandler(EventHandlerABC):
    """
    Event handler for events in dynamodb format
    """

    action: Action
    item_marshaller: MarshallerABC

    def is_usable(self, event: ExternalItemType, context) -> bool:
        # noinspection PyBroadException
        try:
            # noinspection PyTypeChecker
            result = bool(event["Records"][0]["dynamodb"])
            return result
        except Exception:
            return False

    def handle(self, event: ExternalItemType, context) -> ExternalType:
        deserializer = TypeDeserializer()
        for record in event["Records"]:
            # noinspection PyTypeChecker
            new_image = record["NewImage"]
            if new_image:
                new_image = deserializer.deserialize(new_image)
                new_image = self.item_marshaller.load(new_image)
            # noinspection PyTypeChecker
            old_image = record["OldImage"]
            if old_image:
                old_image = deserializer.deserialize(old_image)
                old_image = self.item_marshaller.load(old_image)
            if old_image and new_image:
                if _has_trigger(self.action, AfterUpdateTrigger):
                    self.action.fn(old_image, new_image)
            elif new_image:
                if _has_trigger(self.action, AfterCreateTrigger):
                    self.action.fn(new_image)
            else:
                if _has_trigger(self.action, AfterDeleteTrigger):
                    self.action.fn(old_image)
        return None


@dataclass
class DynamodbPostProcessEventHandlerFactory(EventHandlerFactoryABC):
    marshaller_context: MarshallerContext = attr(default_factory=get_default_context)

    def create(self, action: Action) -> Optional[EventHandlerABC]:
        if (
            _has_trigger(action, AfterCreateTrigger)
            or _has_trigger(action, AfterUpdateTrigger)
            or _has_trigger(action, AfterDeleteTrigger)
        ):
            sig = inspect.signature(action.fn)
            item_type = next(iter(sig.parameters.values())).annotation
            item_marshaller = self.marshaller_context.get_marshaller(item_type)
            return DynamodbPostProcessEventHandler(action, item_marshaller)


def _has_trigger(action_: Action, trigger_type: Type):
    result = next(
        (True for t in action_.triggers if isinstance(t, trigger_type)), False
    )
    return result
