from servey.action.action import action
from dataclasses import dataclass
from typing import Optional

from servey.security.authorization import Authorization

from persisty.result_set import ResultSet
from servey_main.models.message import Message
from servey_main.models.user import User


@dataclass
class IndexPageModel:
    user: Optional[User]
    messages: ResultSet[Message]


@action
def index_page(authorization: Optional[Authorization] = None) -> IndexPageModel:
    return IndexPageModel()
