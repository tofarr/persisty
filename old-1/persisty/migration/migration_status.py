from enum import Enum


class MigrationStatus(Enum):
    PENDING = 'pending'
    ERROR = 'error'
    COMPLETE = 'complete'
