from persisty.data_storage.data_meta import DataMeta
from persisty.obj_storage.stored import stored


@stored
class DataItem:
    meta: DataMeta
    data: bytes
