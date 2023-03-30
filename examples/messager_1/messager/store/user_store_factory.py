from persisty.factory.default_store_factory import DefaultStoreFactory

from messager.store import user_store

user_store_factory = DefaultStoreFactory(user_store)
