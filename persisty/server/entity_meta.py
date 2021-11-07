from dataclasses import dataclass

from persisty.capabilities import Capabilities
from persisty.store_schemas import StoreSchemas


@dataclass
class EntityMeta:
    name: str
    capabilities: Capabilities
    schemas: StoreSchemas
