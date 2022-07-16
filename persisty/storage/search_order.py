from dataclasses import dataclass, Field
from typing import Any, Tuple

from marshy.types import ExternalItemType

from persisty.util.undefined import UNDEFINED


@dataclass(frozen=True)
class SearchOrder:
    field_names: Tuple[str, ...]

    def validate_for_fields(self, fields: Tuple[Field, ...]):
        missing_field_names = set(self.field_names) - set(f.name for f in fields)
        if missing_field_names:
            raise ValueError(f'query_filter_invalid_for_fields:{missing_field_names}')

    def key(self, item: ExternalItemType) -> Any:
        key = []
        for f in self.field_names:
            value = item.get(f, UNDEFINED)
            if value in (None, UNDEFINED):
                key.append(True)
                key.append('')
            else:
                key.append(False)  # Positive values are sorted before negative ones
                key.append(value)
        return key


NO_ORDER = SearchOrder(tuple())
