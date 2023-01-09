from servey.action.action import action
from servey.security.authorization import ROOT
from servey.trigger.web_trigger import WEB_POST

from persisty.io import export_all
from persisty.servey import add_actions_for_all_storage_factories

add_actions_for_all_storage_factories(globals())


@action(triggers=(WEB_POST,))
def export_seeds() -> bool:
    export_all("exported", ROOT)
    return True
