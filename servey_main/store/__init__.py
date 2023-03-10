from persisty.factory.default_store_factory import DefaultStoreFactory
from persisty.impl.default_store import DefaultStore
from persisty.store.restrict_access_store import RestrictAccessStore
from persisty.store_access import StoreAccess, READ_ONLY
from persisty.store_meta import get_meta
from persisty_data.directory_data_store import directory_data_store

from servey_main.models.message import Message
from servey_main.models.user import User

user_store = DefaultStore(get_meta(User))
message_store = DefaultStore(get_meta(Message))

user_image_data_store = directory_data_store('user_image')
