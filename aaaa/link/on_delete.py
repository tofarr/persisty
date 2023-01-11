from enum import Enum


class OnDelete(Enum):
    BLOCK = "BLOCK"
    CASCADE = "CASCADE"
    NULLIFY = "NULLIFY"
