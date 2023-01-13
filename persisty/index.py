from dataclasses import dataclass
from typing import Tuple


@dataclass
class Index:
    attr_names: Tuple[str, ...]
    unique: bool = False
