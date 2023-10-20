import json
from dataclasses import dataclass, field
from typing import Optional, Type

import marshy
from marshy.marshaller_context import MarshallerContext
from marshy.types import ExternalItemType

from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_filter.search_filter_factory import SearchFilterFactoryABC
from persisty.security.store_access import StoreAccess
from persisty.store_meta import StoreMeta


@dataclass(frozen=True)
class NamedPermission:
    """Permission with a name. The name is typically that of a store or a scope"""

    name: str
    create_filter: Optional[str] = None
    read_filter: Optional[str] = None
    update_filter: Optional[str] = None
    delete_filter: Optional[str] = None
    searchable: bool = True

    def to_store_access(
        self,
        store_meta: StoreMeta,
        marshaller_context: Optional[MarshallerContext] = None,
    ):
        if not marshaller_context:
            marshaller_context = marshy.get_default_context()
        search_filter_factory_type = store_meta.get_search_filter_factory_dataclass()
        store_access = StoreAccess(
            create_filter=_load_filter(self.create_filter, search_filter_factory_type, marshaller_context),
            read_filter=_load_filter(self.read_filter, search_filter_factory_type, marshaller_context),
            update_filter=_load_filter(self.update_filter, search_filter_factory_type, marshaller_context),
            delete_filter=_load_filter(self.delete_filter, search_filter_factory_type, marshaller_context),
            searchable=self.searchable,
        )
        return store_access


def _load_filter(
    filter_definition: Optional[str],
    search_filter_factory_type,
    marshaller_context: MarshallerContext
) -> SearchFilterABC:
    if not filter_definition:
        return EXCLUDE_ALL
    filter_json = json.loads(filter_definition)
    filter_factory = marshaller_context.load(search_filter_factory_type, filter_json)
    search_filter = filter_factory.to_search_filter()
    return search_filter
