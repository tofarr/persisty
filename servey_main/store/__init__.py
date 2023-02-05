from persisty.factory.default_store_factory import DefaultStoreFactory
from persisty.impl.default_store import DefaultStore
from persisty.store.restrict_access_store import RestrictAccessStore
from persisty.store_access import StoreAccess, READ_ONLY
from persisty.store_meta import get_meta
from persisty.stored import stored
from persisty_data.chunk import Chunk
from persisty_data.content_meta import ContentMeta
from persisty_data.data_store import wrap_stores

from persisty_data.upload import Upload
from servey_main.models.message import Message
from servey_main.models.user import User

user_store = DefaultStore(get_meta(User))
message_store = DefaultStore(get_meta(Message))


@stored
class UserImageContentMeta(ContentMeta):
    pass


@stored
class UserImageChunk(Chunk):
    pass


@stored
class UserImageUpload(Upload):
    pass


user_image_content_meta_store, user_image_chunk_store, user_image_upload_store = wrap_stores(
    DefaultStore(get_meta(UserImageContentMeta)),
    DefaultStore(get_meta(UserImageChunk)),
    DefaultStore(get_meta(UserImageUpload))
)

user_image_content_meta_store_factory = DefaultStoreFactory(
    RestrictAccessStore(user_image_content_meta_store, StoreAccess(creatable=False, updatable=False))
)

user_image_chunk_store_factory = DefaultStoreFactory(
    RestrictAccessStore(user_image_chunk_store, READ_ONLY)
)
