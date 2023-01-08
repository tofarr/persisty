from servey.trigger.trigger_abc import TriggerABC


class AfterUpdateTrigger(TriggerABC):
    storage_name: str
