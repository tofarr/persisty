from abc import ABC
from dataclasses import dataclass
from typing import Optional, Iterator, Type, TypeVar

from persisty.capabilities import Capabilities
from persisty.edit import Edit
from persisty.page import Page
from persisty.repo_abc import RepoABC, F, T


T = TypeVar('T')


@dataclass(frozen=True)
class WrapperRepoABC(RepoABC[T], ABC):
    """ Abstract Wrapper for wrapping a repository to alter functionality """
    repo: RepoABC[T]
    name: str = None

    def __post_init__(self):
        if self.name is None:
            object.__setattr__(self, 'name', self.repo.name)
    
    def get_item_type(self) -> Type[T]:
        return self.repo.get_item_type()

    def get_capabilities(self) -> Capabilities:
        return self.repo.get_capabilities()

    def get_key(self, item: T) -> str:
        return self.repo.get_key(item)

    def create(self, item: T) -> str:
        return self.repo.create(item)

    def read(self, key: str) -> Optional[T]:
        return self.repo.read(key)

    def read_all(self, keys: Iterator[str], error_on_missing: bool = True) -> Iterator[T]:
        return self.repo.read_all(keys, error_on_missing)

    def update(self, item: T) -> T:
        return self.repo.update(item)

    def destroy(self, key: str) -> bool:
        return self.repo.destroy(key)

    def search(self, search_filter: Optional[F] = None) -> Iterator[T]:
        return self.repo.search(search_filter)

    def count(self, search_filter: Optional[F] = None) -> int:
        return self.repo.count(search_filter)

    def paginated_search(self,
                         search_filter: Optional[F] = None,
                         page_key: str = None,
                         limit: int = 20
                         ) -> Page[T]:
        return self.repo.paginated_search(search_filter, page_key, limit)

    def edit_all(self, edits: Iterator[Edit[T]]):
        return self.repo.edit_all(edits)
