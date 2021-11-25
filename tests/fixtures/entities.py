from typing import ForwardRef, Iterable

from persisty.attr.belongs_to_attr import BelongsToAttr
from persisty.attr.has_many_attr import HasManyAttr
from persisty.entity.entity_abc import EntityABC
from tests.fixtures.item_types import Member, MemberFilter, Band, BandFilter

BAND_ENTITY_CLASS = ForwardRef(f'{__name__}.BandEntity')
MEMBER_ENTITY_CLASS = ForwardRef(f'{__name__}.MemberEntity')


class MemberEntity(Member, EntityABC):
    band: BAND_ENTITY_CLASS = BelongsToAttr()
    __filter_class__ = MemberFilter


class BandEntity(Band, EntityABC):
    members: Iterable[MEMBER_ENTITY_CLASS] = HasManyAttr()
    __filter_class__ = BandFilter
