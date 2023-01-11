from dataclasses import dataclass

from servey.trigger.trigger_abc import TriggerABC


@dataclass
class AfterDeleteTrigger(TriggerABC):
    store_name: str
