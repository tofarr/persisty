from enum import Enum


class FieldType(Enum):
    BINARY = "binary"
    BOOL = "bool"
    DATETIME = "datetime"
    FLOAT = "float"
    INT = "int"
    JSON = "json"
    STR = "str"
    UUID = "uuid"
