from persisty.factory.default_store_factory import DefaultStoreFactory

from messager.store import message_store

message_store_factory = DefaultStoreFactory(message_store)
