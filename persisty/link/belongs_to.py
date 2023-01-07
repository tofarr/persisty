from dataclasses import dataclass
from typing import Callable, Optional, ForwardRef, Any, Dict, Type, List

from marshy.factory.optional_marshaller_factory import get_optional_type
from marshy.types import ExternalItemType
from schemey.schema import str_schema
from servey.action.action import action
from servey.action.batch_invoker import BatchInvoker
from servey.finder.action_finder_abc import find_actions
from servey.security.authorization import Authorization

from persisty.field.field import Field
from persisty.field.field_type import FieldType
from persisty.finder.storage_factory_finder_abc import find_storage_factories
from persisty.link.link_abc import LinkABC
from persisty.link.on_delete import OnDelete
from persisty.servey import output
from persisty.util import to_snake_case, UNDEFINED


@dataclass
class BelongsTo(LinkABC):
    name: Optional[str] = None
    private_name: Optional[str] = None
    linked_storage_name: Optional[str] = None
    key_field_name: Optional[str] = None
    optional: Optional[bool] = None
    on_delete: OnDelete = OnDelete.BLOCK

    def get_name(self) -> str:
        return self.name

    def __set_name__(self, owner, name):
        self.name = name
        annotations = owner.__annotations__
        type_ = annotations[name]
        optional = bool(get_optional_type(type_))
        if optional:
            type_ = get_optional_type(type_)
        if self.private_name is None:
            self.private_name = f'_{name}'
        if self.optional is None:
            self.optional = optional
        if self.linked_storage_name is None:
            self.linked_storage_name = to_snake_case(type_ if isinstance(type_, str) else type_.__name__)
        if self.key_field_name is None:
            self.key_field_name = f"{name}_id"

    def update_params(self, params: Dict[str, Any], annotations: Dict[str, Type], fields: List[Field]):
        if self.key_field_name not in annotations:
            fields.append(Field(
                self.key_field_name,
                FieldType.STR,
                str_schema(),
                is_sortable=False,
                is_indexed=True
            ))

    def to_action_fn(self, owner_name: str) -> Callable:
        linked_storage_factory = next(
            f for f in find_storage_factories()
            if f.get_storage_meta().name == self.linked_storage_name
        )
        linked_storage_meta = linked_storage_factory.get_storage_meta()
        key_field_name = self.key_field_name
        linked_type_name = f'persisty.servey.output.{self.linked_storage_name.title()}'
        batch_read_action_name = f"{self.linked_storage_name}_batch_read"
        batch_read_fn_ = None

        async def batch_fn(items):
            nonlocal batch_read_fn_
            if not batch_read_fn_:
                batch_read_fn_ = next(a.fn for a in find_actions() if a.name == batch_read_action_name)
            keys = [getattr(item, key_field_name) for item in items]
            result = batch_read_fn_(keys)
            return result

        return_type = ForwardRef(linked_type_name)
        if self.optional:
            return_type = Optional[return_type]

        @action(
            name=owner_name+'_'+self.name,
            batch_invoker=BatchInvoker(
                fn=batch_fn,
                max_batch_size=linked_storage_meta.batch_size
            )
        )
        def action_fn(self) -> return_type:
            """ Dummy - always batched """

        action_fn.__name__ = owner_name+'_'+self.name
        return action_fn

    def update_json_schema(self, json_schema: ExternalItemType):
        id_field_schema = json_schema.get("properties").get(self.key_field_name)
        id_field_schema["persistyBelongsTo"] = self.linked_storage_name

        id_field_schema = json_schema.get("properties").get(self.key_field_name)
        id_field_schema["persistyBelongsTo"] = self.linked_storage_name

    def __get__(self, instance, owner):
        value = getattr(instance, '_'+self.name, UNDEFINED)
        if value is not UNDEFINED:
            return value
        key = getattr(instance, self.private_name)
        if not key:
            return None
        entity_type = getattr(output, self.private_name)
        value = entity_type.read(str(key), instance.__authorization__)
        if value:
            setattr(instance, self.private_name, value)
        return value

    def after_set_attr(self, instance, key: str, old_value: Any, new_value: Any):
        if key == self.key_field_name:
            # this indicates the intent is to change from belonging from one entity to another
            setattr(instance, self.private_name, UNDEFINED)
