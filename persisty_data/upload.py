
from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from persisty.stored import stored


class UploadStatus(Enum):
    IN_PROGRESS = 'in_progress'
    ABORTED = 'aborted'
    TIMED_OUT = 'timed_out'
    COMPLETED = 'completed'


@stored
class Upload:
    id: str
    status: UploadStatus = UploadStatus.IN_PROGRESS
    content_key: str
    content_type: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    expire_at: datetime
