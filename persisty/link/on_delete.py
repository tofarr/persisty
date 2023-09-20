from enum import Enum


class OnDelete(Enum):
    IGNORE = "IGNORE"
    BLOCK = "BLOCK"
    CASCADE = "CASCADE"
    NULLIFY = "NULLIFY"
