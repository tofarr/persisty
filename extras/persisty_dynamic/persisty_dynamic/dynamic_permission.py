from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List, Set

from servey.security.authorization import Authorization
from servey.util.singleton_abc import SingletonABC


@dataclass
class DynamicPermission:
    public: bool = False
    subject_ids: Optional[Set[str]] = None
    scopes: Optional[Set[str]] = None

    def is_permitted(self, authorization: Optional[Authorization]) -> bool:
        if self.public:
            return True
        if (
            self.subject_ids
            and authorization
            and authorization.subject_id in self.subject_ids
        ):
            return True
        if self.scopes and authorization and bool(authorization.scopes - self.scopes):
            return True
        return False


DENIED = DynamicPermission()
PUBLIC = DynamicPermission(True)
