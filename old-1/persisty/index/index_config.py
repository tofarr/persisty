from dataclasses import dataclass
from typing import Optional


@dataclass
class IndexConfig:
    primary_attr: str
    secondary_attr: Optional[str] = None
    # Did somebody say partition key and sort key?

