from dataclasses import dataclass
from typing import Optional


@dataclass
class Band:
    id: Optional[str] = None
    band_name: Optional[str] = None
    year_formed: Optional[int] = None


@dataclass
class BandFilter:
    query: Optional[str] = None
    year_formed__gt: Optional[int] = None
    year_formed__gte: Optional[int] = None
    year_formed__lt: Optional[int] = None
    year_formed__lte: Optional[int] = None
    year_formed__eq: Optional[int] = None
    year_formed__ne: Optional[int] = None
    band_name__begins_with: Optional[str] = None
    band_name__contains: Optional[str] = None
    sort: Optional[str] = None


@dataclass
class Member:
    id: str
    member_name: str
    band_id: str
    date_of_birth: str


@dataclass
class MemberFilter:
    query: Optional[str] = None
    band_id__eq: Optional[str] = None
    sort: Optional[str] = None
