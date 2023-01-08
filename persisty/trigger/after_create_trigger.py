from servey.trigger.trigger_abc import TriggerABC


class AfterCreateTrigger(TriggerABC):
    storage_name: str
