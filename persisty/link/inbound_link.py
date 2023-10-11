from dataclasses import dataclass

from persisty.link.on_delete import OnDelete
from persisty.store_meta import StoreMeta


@dataclass
class InboundLink:
    store_meta: StoreMeta
    attr_name: str
    on_delete: OnDelete
