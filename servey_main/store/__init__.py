from persisty.factory.default_store_factory import DefaultStoreFactory
from persisty.impl.default_store import DefaultStore
from persisty.store.restrict_access_store import RestrictAccessStore
from persisty.store_access import StoreAccess, READ_ONLY
from persisty.store_meta import get_meta
from persisty_data.data_store import create_default_stores_for_data

from servey_main.models.message import Message
from servey_main.models.user import User

user_store = DefaultStore(get_meta(User))
message_store = DefaultStore(get_meta(Message))

user_image_content_meta_store, user_image_chunk_store, user_image_upload_store = (
    create_default_stores_for_data('user_image')
)

user_image_content_meta_store_factory = DefaultStoreFactory(
    RestrictAccessStore(user_image_content_meta_store, StoreAccess(creatable=False, updatable=False))
)

user_image_chunk_store_factory = DefaultStoreFactory(
    RestrictAccessStore(user_image_chunk_store, READ_ONLY)
)
