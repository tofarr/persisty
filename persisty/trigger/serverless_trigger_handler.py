from dataclasses import field
from typing import Dict

from marshy.types import ExternalItemType
from servey.action.action import Action
from servey.servey_aws.serverless.trigger_handler.trigger_handler_abc import (
    TriggerHandlerABC,
)
from servey.trigger.trigger_abc import TriggerABC

from persisty.trigger.after_create_trigger import AfterCreateTrigger
from persisty.trigger.after_delete_trigger import AfterDeleteTrigger
from persisty.trigger.after_update_trigger import AfterUpdateTrigger


class ServerlessTriggerHandler(TriggerHandlerABC):
    unmanaged_table_arns_by_store_name: Dict[str, str] = field(default_factory=dict)

    def handle_trigger(
        self, action: Action, trigger: TriggerABC, lambda_definition: ExternalItemType
    ):
        if (
            not isinstance(trigger, AfterCreateTrigger)
            and not isinstance(trigger, AfterUpdateTrigger)
            and not isinstance(trigger, AfterDeleteTrigger)
        ):
            return
        stream = lambda_definition.get("stream")
        if not stream:
            stream = lambda_definition["stream"] = []
        arn = self.unmanaged_table_arns_by_store_name.get(trigger.store_name)
        if not arn:
            arn = {
                "Fn::GetAtt": [
                    trigger.store_name.title().replace("_", ""),
                    "StreamArn",
                ]
            }
        stream.append(
            dict(
                type="dynamodb",
                arn=arn,
            )
        )
