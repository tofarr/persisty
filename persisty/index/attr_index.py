from dataclasses import dataclass

from persisty.index.index_abc import IndexABC


@dataclass
class AttrIndex(IndexABC):
    """
    Standard single attribute index, usually translated into a btree and used for boosting performance
    """

    attr_name: str
