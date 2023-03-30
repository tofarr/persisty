from persisty.impl.default_store import DefaultStore
from persisty.store_meta import get_meta

from messager.models.message import Message
from messager.models.user import User

user_store = DefaultStore(get_meta(User))
message_store = DefaultStore(get_meta(Message))
