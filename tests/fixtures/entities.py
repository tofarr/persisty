from typing import ForwardRef, Iterable

from persisty.obj_graph import EntityABC
from persisty.obj_graph import BelongsTo
from persisty.obj_graph import HasMany
from tests.fixtures.items import Member, Band, MemberFilter, BandFilter

BAND_ENTITY_CLASS = ForwardRef(f'{__name__}.BandEntity')
MEMBER_ENTITY_CLASS = ForwardRef(f'{__name__}.MemberEntity')


class MemberEntity(EntityABC, Member):
    band: BAND_ENTITY_CLASS = BelongsTo(key_attr='band_id')
    __filter_class__ = MemberFilter


class BandEntity(EntityABC, Band):
    members: Iterable[MEMBER_ENTITY_CLASS] = HasMany(foreign_key_attr='band_id')
    __filter_class__ = BandFilter
