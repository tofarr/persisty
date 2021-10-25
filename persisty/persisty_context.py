import importlib
import os
from typing import Union, Type, TypeVar, Iterator

from persisty.errors import PersistyError
from persisty.repo_abc import RepoABC

T = TypeVar('T')
F = TypeVar('F')


class PersistyContext:

    def __init__(self):
        self._repos_by_name = {}

    def register_repo(self, repo: RepoABC[T, F]):
        self._repos_by_name[repo.name] = repo

    def get_repo(self, key: Union[str, Type[T]]) -> RepoABC[T, F]:
        if not isinstance(key, str):
            key = key.__name__
        repo = self._repos_by_name.get(key)
        if repo is None:
            raise PersistyError(f'missing_repo:{key}')
        return repo

    def get_repos(self) -> Iterator[RepoABC]:
        return iter(self._repos_by_name.values())
