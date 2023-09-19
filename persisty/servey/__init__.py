from typing import Dict, Iterator

from servey.action.action import Action


def create_actions_for_all_stores() -> Iterator[Action]:
    from persisty.finder.stored_finder_abc import find_stored

    for store_meta in find_stored():
        store = store_meta.store_factory.create(store_meta)
        yield from store_meta.action_factory.create_actions(store)


def add_actions_for_all_stores(target: Dict):
    for action_ in create_actions_for_all_stores():
        target[action_.name] = action_.fn
