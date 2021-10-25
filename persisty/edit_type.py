from enum import Enum


class EditType(Enum):
    CREATE = 'create'
    UPDATE = 'update'
    DESTROY = 'destroy'
