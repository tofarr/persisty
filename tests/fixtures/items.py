from dataclasses import dataclass
from typing import Optional, Union, List


@dataclass(unsafe_hash=True)
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
    band_name__startswith: Optional[str] = None
    band_name__contains: Optional[str] = None
    sort: Optional[List[str]] = None


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


@dataclass
class Issue:
    title: str
    # Optional because an entity that has not yet been created in the database will not have these
    id: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass
class IssueFilter:
    query: Optional[str] = None
    updated_at__gte: Optional[str] = None
    sort: Optional[str] = None
