from typing import Dict

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.servey import generated


def add_actions_for_all_store_factories(target: Dict):
    from persisty.finder.store_finder_abc import find_store_factories

    for store_factory in find_store_factories():
        add_actions_for_store_factory(store_factory, target)


def add_actions_for_store_factory(store_factory: StoreFactoryABC, target: Dict):
    for action_ in store_factory.create_actions():
        target[action_.name] = action_.fn
