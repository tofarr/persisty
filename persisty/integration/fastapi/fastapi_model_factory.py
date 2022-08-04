from dataclasses import dataclass
from typing import Type

from pydantic.dataclasses import dataclass as pydantic_dataclass

from persisty.context import PersistyContext
from persisty.integration.pydantic import storage_meta_to_pydantic_model
from persisty.search_filter.search_filter_factory import search_filter_dataclass_for
from persisty.search_order.search_order_factory import search_order_dataclass_for
from persisty.storage.result_set import result_set_dataclass_for
from persisty.storage.storage_meta import StorageMeta
from persisty.util import to_camel_case, UNDEFINED


@dataclass
class FastApiModelFactory:
    storage_meta: StorageMeta
    persisty_context: PersistyContext

    @property
    def item_model(self) -> Type:
        item_model = getattr(self, "_item_model", None)
        if not item_model:
            item_model = storage_meta_to_pydantic_model(self.storage_meta)
            setattr(self, "_item_model", item_model)
        return item_model

    @property
    def create_input_model(self) -> Type:
        create_input_model = getattr(self, "_create_input_model", None)
        if not create_input_model:
            create_input_model = storage_meta_to_pydantic_model(
                name=f"Create{to_camel_case(self.storage_meta.name)}Input",
                storage_meta=self.storage_meta,
                field_check=lambda f: f.is_creatable,
            )
            setattr(self, "_create_input_model", create_input_model)
        return create_input_model

    @property
    def update_input_model(self) -> Type:
        update_input_model = getattr(self, "_update_input_model", None)
        if not update_input_model:
            update_input_model = storage_meta_to_pydantic_model(
                name=f"Update{to_camel_case(self.storage_meta.name)}Input",
                storage_meta=self.storage_meta,
                field_check=lambda f: f.is_updatable,
            )
            setattr(self, "_update_input_model", update_input_model)
        return update_input_model

    @property
    def search_filter_model(self) -> Type:
        search_filter_model = getattr(self, "_search_filter_model", None)
        if not search_filter_model:
            search_filter_model = pydantic_dataclass(
                search_filter_dataclass_for(self.storage_meta)
            )
            setattr(self, "_search_filter_model", search_filter_model)
        return search_filter_model

    @property
    def search_order_model(self) -> Type:
        search_order_model = getattr(self, "_search_order_model", UNDEFINED)
        if search_order_model is UNDEFINED:
            search_order_model = pydantic_dataclass(
                search_order_dataclass_for(self.storage_meta)
            )
            setattr(self, "_search_order_model", search_order_model)
        return search_order_model

    @property
    def result_set_model(self) -> Type:
        result_set_model = getattr(self, "_result_set_model", UNDEFINED)
        if result_set_model is UNDEFINED:
            # noinspection PyTypeChecker
            result_set_model = pydantic_dataclass(
                result_set_dataclass_for(self.item_model)
            )
            setattr(self, "_result_set_model", result_set_model)
        return result_set_model
