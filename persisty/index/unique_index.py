from dataclasses import dataclass
from typing import Tuple

from persisty.index.index_abc import IndexABC


@dataclass
class UniqueIndex(IndexABC):
    """Unique attribute index"""

    attr_names: Tuple[str, ...]


def unique_index(*args):
    return UniqueIndex(args)
