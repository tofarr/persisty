from dataclasses import dataclass, field
from typing import Optional, Type, Generic, TypeVar
from uuid import UUID, uuid4

from persisty.key_config.key_config_abc import KeyConfigABC

C = TypeVar("C")
U = TypeVar("U")


@dataclass
class BatchEdit(Generic[C, U]):
    """
    Batch edit should define one of create_item, update_item, or delete_key. Defining multiple is not valid.
    """

    id: UUID = field(default_factory=uuid4)
    create_item: Optional[C] = None
    update_item: Optional[U] = None
    delete_key: Optional[str] = None

    def get_key(self, key_config: KeyConfigABC) -> str:
        if self.create_item:
            return key_config.to_key_str(self.create_item)
        if self.update_item:
            return key_config.to_key_str(self.update_item)
        else:
            return self.delete_key


def batch_edit_dataclass_for(
    type_name: str, create_input_type: Type, update_input_type: Type
) -> Type:
    params = {
        "__annotations__": {
            "create_item": Optional[create_input_type],
            "update_item": Optional[update_input_type],
            "delete_key": Optional[str],
        },
        "create_item": None,
        "update_item": None,
        "delete_key": None,
    }
    type_ = dataclass(type(type_name, (), params))
    return type_
