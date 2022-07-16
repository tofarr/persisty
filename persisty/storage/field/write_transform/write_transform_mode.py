from enum import Enum


class WriteTransformMode(Enum):
    ALWAYS_FOR_CREATE = 'always_on_create'
    ALWAYS_FOR_UPDATE = 'always_on_update'
    ALWAYS_FOR_WRITE = 'always_on_write'
    OPTIONAL_FOR_CREATE = 'optional_on_create'
    OPTIONAL_FOR_UPDATE = 'optional_on_update'
    OPTIONAL_FOR_WRITE = 'optional_on_write'
