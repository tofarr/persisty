from typing import List

from marshy import dump

from persisty.key_config.field_key_config import FieldKeyConfig
from persisty.obj_storage.attr import Attr
from persisty.obj_storage.stored import stored
from persisty.storage.field.field_type import FieldType


@stored(key_config=FieldKeyConfig('code', FieldType.STR))
class Band:
    code: str = Attr()
    name: str = Attr()
    year_formed: int


BANDS = [
    Band('beatles', 'The Beatles', 1960),
    Band(code='jefferson_airplane', name='Jefferson Airplane', year_formed=1965)
]
BAND_DICTS = dump(BANDS, List[Band])

EXTRA_BAND = Band(code='rolling_stones', name='The Rolling Stones', year_formed=1962)
EXTRA_BAND_DICT = dump(EXTRA_BAND)
