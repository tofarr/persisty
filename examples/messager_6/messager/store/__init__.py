from persisty.impl.default_store import DefaultStore
from persisty.store_meta import get_meta
from persisty_data.default_data_store import default_data_store

from messager.models.message import Message
from messager.models.user import User

user_store = DefaultStore(get_meta(User))
message_store = DefaultStore(get_meta(Message))

user_image_data_store = default_data_store("user_image", globals())
