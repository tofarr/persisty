from datetime import datetime
from enum import Enum
from uuid import UUID


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
ATTR_TYPE_MAP = {v: k for k, v in TYPE_MAP.items()}


def attr_type(type_) -> AttrType:
    return TYPE_MAP.get(type_) or AttrType.JSON
