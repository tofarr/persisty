from dataclasses import dataclass
from typing import Any, Union, Iterable, Sized


@dataclass(frozen=True)
class Where:
    sql: str
    params: Union[Iterable[Any], Sized]
    pure_sql: bool
