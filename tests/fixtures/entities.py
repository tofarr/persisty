from typing import ForwardRef, Iterable

from persisty.obj_graph.entity_abc import EntityABC
from persisty.obj_graph.resolver.belongs_to import BelongsTo
from persisty.obj_graph.resolver.has_many import HasMany
from tests.fixtures.items import Member, Band

BAND_ENTITY_CLASS = ForwardRef(f'{__name__}.BandEntity')
MEMBER_ENTITY_CLASS = ForwardRef(f'{__name__}.MemberEntity')


class MemberEntity(EntityABC, Member):
    band: BAND_ENTITY_CLASS = BelongsTo(key_attr='band_id')


class BandEntity(EntityABC, Band):
    members: Iterable[MEMBER_ENTITY_CLASS] = HasMany(foreign_key_attr='band_id', inverse_attr='_band')
