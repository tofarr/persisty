from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, TypeVar, Generic, Type

from persisty.storage.batch_edit import BatchEditABC

T = TypeVar('T')


@dataclass
class BatchEditResult(Generic[T]):
    edit: BatchEditABC[T]
    success: bool = False
    code: Optional[str] = None
    details: Optional[str] = None

    def copy_from(self, result: BatchEditResult):
        self.edit = result.edit
        self.success = result.success
        self.code = result.code
        self.details = result.details
