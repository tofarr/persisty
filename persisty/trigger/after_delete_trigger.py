from servey.trigger.trigger_abc import TriggerABC


class AfterDeleteTrigger(TriggerABC):
    storage_name: str
