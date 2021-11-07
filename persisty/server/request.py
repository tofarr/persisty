from dataclasses import dataclass
from typing import Dict, List, Optional

from marshy import ExternalType


@dataclass
class Request:
    method: str
    path: List[str]
    headers: Dict[str, str]
    params: Dict[str, str]
    input: Optional[ExternalType] = None
