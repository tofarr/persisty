import dataclasses

from servey.security.authorizer.authorizer_factory_abc import get_default_authorizer

from persisty.factory.default_store_factory import DefaultStoreFactory
from persisty_data.data_store import DataStore
from servey_main.store import user_image_upload_store, user_image_chunk_store, user_image_content_meta_store

user_image_data_store_factory = DataStore(
    name="user_image",
    content_meta_store_factory=DefaultStoreFactory(user_image_content_meta_store),
    upload_store_factory=DefaultStoreFactory(user_image_upload_store),
    chunk_store_factory=DefaultStoreFactory(user_image_chunk_store),
    upload_store=user_image_upload_store,
    authorizer=dataclasses.replace(get_default_authorizer(), aud='data_url'),
    secured_upload_path="/data/user-image",
    public_download_path="/data/user-image/{key}",
)