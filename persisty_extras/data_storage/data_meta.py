from datetime import datetime
from typing import Optional

from persisty.obj_storage.stored import stored, get_storage_meta
from persisty.search_filter.search_filter_factory import search_filter_dataclass_for


@stored
class DataMeta:
    key: str
    updated_at: datetime
    size_in_bytes: int
    mime_type: Optional[str] = None
    etag: Optional[str] = None


DataMetaFilter = search_filter_dataclass_for(get_storage_meta(DataMeta))
