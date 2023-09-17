from dataclasses import dataclass
from typing import Optional

from marshy.types import ExternalItemType

from persisty.store_access import StoreAccess, NO_ACCESS


@dataclass(frozen=True)
class Permission:
    store_access: StoreAccess = NO_ACCESS
    search_filter: Optional[ExternalItemType] = None
