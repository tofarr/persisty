from dataclasses import dataclass

from persisty.security.store_access import StoreAccess


@dataclass(frozen=True)
class NamedPermission:
    """Permission with a name. The name is typically that of a store or a scope"""

    name: str
    store_access: StoreAccess
