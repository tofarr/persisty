from enum import Enum


class OnDestroy(Enum):
    NO_ACTION = 'no_action'
    NULLIFY = 'nullify'
    CASCADE = 'cascade'
