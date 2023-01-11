from datetime import datetime
from enum import Enum
from uuid import UUID

from marshy.factory.optional_marshaller_factory import get_optional_type


class AttrType(Enum):
    BINARY = "binary"
    BOOL = "bool"
    DATETIME = "datetime"
    FLOAT = "float"
    INT = "int"
    JSON = "json"
    STR = "str"
    UUID = "uuid"


TYPE_MAP = {
    bool: AttrType.BOOL,
    datetime: AttrType.DATETIME,
    float: AttrType.FLOAT,
    int: AttrType.INT,
    str: AttrType.STR,
    UUID: AttrType.UUID,
}


def attr_type(type_) -> AttrType:
    return TYPE_MAP.get(type_) or AttrType.JSON
