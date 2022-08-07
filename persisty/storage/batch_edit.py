from dataclasses import dataclass, field
from typing import Optional
from uuid import UUID, uuid4

from marshy.types import ExternalItemType

from persisty.key_config.key_config_abc import KeyConfigABC


@dataclass
class BatchEdit:
    """
    Batch edit should define one of create_item, update_item, or delete_key. Defining multiple is not valid.
    """

    id: UUID = field(default_factory=uuid4)
    create_item: Optional[ExternalItemType] = None
    update_item: Optional[ExternalItemType] = None
    delete_key: Optional[str] = None

    def get_key(self, key_config: KeyConfigABC) -> str:
        if self.create_item:
            return key_config.to_key_str(self.create_item)
        if self.update_item:
            return key_config.to_key_str(self.update_item)
        else:
            return self.delete_key
