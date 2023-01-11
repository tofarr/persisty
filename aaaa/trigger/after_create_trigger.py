from dataclasses import dataclass

from servey.trigger.trigger_abc import TriggerABC


@dataclass
class AfterCreateTrigger(TriggerABC):
    store_name: str
