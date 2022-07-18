from abc import abstractmethod, ABC
from typing import Any, Tuple

from dataclasses import dataclass
from marshy.types import ExternalItemType, ExternalType

from persisty.storage.field.field import Field
from persisty.util.undefined import UNDEFINED


class SearchOrderABC(ABC):
    @abstractmethod
    @property
    def field(self) -> str:
        """ Get the field """

    @abstractmethod
    @property
    def desc(self) -> bool:
        """ Get the field """

    def validate_for_fields(self, fields: Tuple[Field, ...]):
        for f in fields:
            if f.name == self.field:
                return
        raise ValueError(f'search_order_invalid:{self.field}')

    def key(self, item: ExternalItemType) -> Any:
        value: ExternalType = item.get(self.field, UNDEFINED)
        if value in (None, UNDEFINED):
            return not self.desc, ''
        else:
            return self.desc, value


@dataclass(frozen=True)
class SearchOrder(SearchOrderABC):
    field: str
    desc: bool = False
