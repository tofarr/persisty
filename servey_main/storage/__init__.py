from persisty.impl.default_storage_factory import DefaultStorageFactory
from persisty.obj_storage.stored import get_storage_meta
from servey_main.models.message import Message
from servey_main.models.user import User

user_storage_factory = DefaultStorageFactory(get_storage_meta(User))
message_storage_factory = DefaultStorageFactory(get_storage_meta(Message))
