from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Type

from persisty.storage.batch_edit import BatchEdit


@dataclass
class BatchEditResult:
    edit: BatchEdit
    success: bool = False
    code: Optional[str] = None
    details: Optional[str] = None

    def copy_from(self, result: BatchEditResult):
        self.edit = result.edit
        self.success = result.success
        self.code = result.code
        self.details = result.details


def batch_edit_result_dataclass_for(batch_edit_type: Type) -> Type:
    params = {
        "__annotations__": {
            "edit": batch_edit_type,
            "success": bool,
            "code": Optional[str],
            "details": Optional[str],
        },
        "success": False,
        "code": None,
        "details": None,
    }
    type_name = f"{batch_edit_type.__name__}Result"
    type_ = dataclass(type(type_name, (), params))
    return type_
