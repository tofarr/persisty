from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List
from uuid import UUID, uuid4


@dataclass(unsafe_hash=True)
class Band:
    id: Optional[str] = None
    title: str = ''
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
    title__startswith: Optional[str] = None
    title__contains: Optional[str] = None
    sort: Optional[List[str]] = None


@dataclass
class Member:
    id: Optional[str] = None
    name: str = ''
    band_id: str = None
    date_of_birth: str = None


@dataclass
class MemberFilter:
    query: Optional[str] = None
    band_id__eq: Optional[str] = None
    sort: Optional[str] = None


@dataclass
class Tag:
    title: str
    id: Optional[UUID] = field(default_factory=uuid4)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class Node:
    title: str
    id: Optional[UUID] = field(default_factory=uuid4)
    parent_id: Optional[UUID] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class NodeFilter:
    sort: Optional[str] = None


@dataclass
class NodeTag:
    node_id: UUID
    tag_id: UUID
    id: Optional[UUID] = field(default_factory=uuid4)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
