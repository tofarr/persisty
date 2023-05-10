from persisty.factory.default_store_factory import DefaultStoreFactory
from persisty.factory.owned_store_factory import OwnedStoreFactory

from messager.store import message_store

message_store_factory = OwnedStoreFactory(
    store_factory=DefaultStoreFactory(message_store), subject_id_attr_name="author_id"
)
