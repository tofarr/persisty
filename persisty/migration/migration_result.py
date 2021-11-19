from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from persisty.migration.migration_status import MigrationStatus


@dataclass
class CompletedMigration:
    id: str
    status: MigrationStatus
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
