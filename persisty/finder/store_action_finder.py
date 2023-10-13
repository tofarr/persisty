from typing import Iterator

from servey.action.action import Action
from servey.finder.action_finder_abc import ActionFinderABC

from persisty.finder.store_meta_finder_abc import find_store_meta


class StoreActionFinder(ActionFinderABC):
    def find_actions(self) -> Iterator[Action]:
        all_store_meta = list(find_store_meta())
        for store_meta in all_store_meta:
            yield from store_meta.action_factory.create_actions(store_meta)
