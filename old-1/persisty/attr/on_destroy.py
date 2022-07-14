from enum import Enum


class OnDestroy(Enum):
    NOOP = 'noop'
    NULLIFY = 'nullify'
    CASCADE = 'cascade'
