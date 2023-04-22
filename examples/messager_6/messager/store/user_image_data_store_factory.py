from persisty.factory.default_store_factory import DefaultStoreFactory
from persisty_data.hosted_data_store_factory import hosted_data_store_factory
from persisty_data.owned_data_store_factory import OwnedDataStoreFactory

from messager.store import user_image_data_store

user_image_data_store_factory = hosted_data_store_factory(
    OwnedDataStoreFactory(
        data_store_factory=DefaultStoreFactory(user_image_data_store)
    )
)
