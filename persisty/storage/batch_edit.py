from abc import ABC
from dataclasses import dataclass
from typing import TypeVar, Generic

T = TypeVar('T')


class BatchEditABC(ABC):
    pass


@dataclass
class Create(BatchEditABC, Generic[T]):
    item: T


@dataclass
class Update(BatchEditABC, Generic[T]):
    item: T


@dataclass
class Delete(BatchEditABC):
    key: str
