from dataclasses import dataclass
from unittest import TestCase

from marshy import dump

from persisty.impl.mem.mem_store import mem_store
from persisty.obj_store.stored import get_meta
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC
from tests.fixtures.number_name import NUMBER_NAMES, NumberName
from tests.fixtures.store_tst_abc import StoreTstABC
from tests.fixtures.super_bowl_results import SuperBowlResult, SUPER_BOWL_RESULTS


class TestWrapperStore(TestCase, StoreTstABC):
    def new_super_bowl_results_store(self) -> StoreABC:
        store = mem_store(
            get_meta(SuperBowlResult),
            {r.code: dump(r) for r in SUPER_BOWL_RESULTS},
        )
        return WrapperStore(store)

    def new_number_name_store(self) -> StoreABC:
        number_names = (dump(r) for r in NUMBER_NAMES)
        # noinspection PyTypeChecker
        store = mem_store(
            get_meta(NumberName),
            {r["id"]: r for r in number_names},
        )
        return WrapperStore(store)


@dataclass
class WrapperStore(WrapperStoreABC):
    store: StoreABC

    def get_store(self) -> StoreABC:
        return self.store
