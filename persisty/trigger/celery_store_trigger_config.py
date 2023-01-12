from typing import Dict

from servey.finder.action_finder_abc import find_actions_with_trigger_type
from servey.servey_celery.celery_config.celery_config_abc import CeleryConfigABC
from celery import Celery

from persisty.trigger.after_create_trigger import AfterCreateTrigger
from persisty.trigger.after_delete_trigger import AfterDeleteTrigger
from persisty.trigger.after_update_trigger import AfterUpdateTrigger


class CeleryStoreTriggerConfig(CeleryConfigABC):
    def configure(self, app: Celery, global_ns: Dict):
        _configure_for_trigger_type(app, global_ns, AfterCreateTrigger)
        _configure_for_trigger_type(app, global_ns, AfterUpdateTrigger)
        _configure_for_trigger_type(app, global_ns, AfterDeleteTrigger)


def _configure_for_trigger_type(app: Celery, global_ns: Dict, trigger_type):
    for _action, _ in find_actions_with_trigger_type(trigger_type):
        if _action.name not in global_ns:
            global_ns[_action.name] = app.task(_action.fn)
