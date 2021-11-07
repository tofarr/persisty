from dataclasses import dataclass, field
from typing import Dict, Optional

from marshy import ExternalType


@dataclass
class Response:
    code: int
    headers: Dict[str, str] = field(default_factory=Dict)
    content: Optional[ExternalType] = None
