from persisty.impl.default_store_factory import DefaultStoreFactory
from persisty.store_meta import get_meta
from servey_main.models.message import Message
from servey_main.models.user import User

user_store_factory = DefaultStoreFactory(get_meta(User))
message_store_factory = DefaultStoreFactory(get_meta(Message))
