from dataclasses import dataclass
from typing import Optional, ForwardRef, List

from persisty import PersistyContext
from persisty.mem.mem_repo import mem_repo
from persisty.obj_graph.entity_abc import EntityABC
from persisty.obj_graph.resolver.belongs_to import BelongsTo
from persisty.obj_graph.resolver.has_many import HasMany


@dataclass
class Band:
    id: str
    band_name: str


@dataclass
class BandFilter:
    query: Optional[str] = None


@dataclass
class Member:
    id: str
    member_name: str
    band_id: str


@dataclass
class MemberFilter:
    query: Optional[str] = None
    band_id__eq: Optional[str] = None


persisty_context = PersistyContext()
persisty_context.register_repo(mem_repo(Band, BandFilter))
persisty_context.register_repo(mem_repo(Member, MemberFilter))

BAND_ENTITY_CLASS = ForwardRef(f'{__name__}.BandEntity')
MEMBER_ENTITY_CLASS = ForwardRef(f'{__name__}.MemberEntity')


class MemberEntity(EntityABC, Member):
    __persisty_context__ = persisty_context
    band: BAND_ENTITY_CLASS = BelongsTo(key_attr='band_id')


class BandEntity(EntityABC, Band):
    __persisty_context__ = persisty_context
    members: List[MEMBER_ENTITY_CLASS] = HasMany(foreign_key_attr='band_id', search_filter_type=MemberFilter,
                                                 inverse_attr='_band')


if __name__ == '__main__':
    bands = [
        BandEntity('beatles', 'The Beatles'),
        BandEntity('rolling_stones', 'The Rolling Stones'),
        BandEntity('led_zeppelin', 'Led Zeppelin')
    ]
    for band in bands:
        band.save()

    members = [
        MemberEntity('john', 'John Lennon', 'beatles'),
        MemberEntity('paul', 'Paul McCartney', 'beatles'),
        MemberEntity('george', 'George Harrison', 'beatles'),
        MemberEntity('ringo', 'Ringo Starr', 'beatles'),
        MemberEntity('jagger', 'Mick Jagger', 'rolling_stones'),
        MemberEntity('jones', 'Brian Jones', 'rolling_stones'),
        MemberEntity('richards', 'Kieth Richards', 'rolling_stones'),
        MemberEntity('wyman', 'Bill Wyman', 'rolling_stones'),
        MemberEntity('watts', 'Charlie Watts', 'rolling_stones'),
        MemberEntity('plant', 'Robert Plant', 'led_zeppelin'),
        MemberEntity('page', 'Jimmy Page', 'led_zeppelin'),
        MemberEntity('jones', 'John Paul Jones', 'led_zeppelin'),
        MemberEntity('bonham', 'John Bonham', 'led_zeppelin')
    ]
    for member in members:
        member.save()

    member = MemberEntity.read('john')
    print(member)
    print(member.band_id)
    print(member.member_name)
    print(member.band)
    print(member.band.members)

    for band in BandEntity.search():
        print(band)
        print(band.members)
