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
    bytes: AttrType.BINARY,
    datetime: AttrType.DATETIME,
    float: AttrType.FLOAT,
    int: AttrType.INT,
    str: AttrType.STR,
    UUID: AttrType.UUID,
}
ATTR_TYPE_MAP = {v: k for k, v in TYPE_MAP.items()}


def attr_type(type_) -> AttrType:
    type_ = get_optional_type(type_) or type_
    result = TYPE_MAP.get(type_)
    if result:
        return result
    try:
        if issubclass(type_, Enum):
            return AttrType.STR
    except TypeError:
        pass
    return AttrType.JSON
