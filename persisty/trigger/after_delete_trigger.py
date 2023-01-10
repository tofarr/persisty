from dataclasses import dataclass

from servey.trigger.trigger_abc import TriggerABC


@dataclass
class AfterDeleteTrigger(TriggerABC):
    storage_name: str
