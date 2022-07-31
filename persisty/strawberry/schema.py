from typing import Optional, Dict

import strawberry

from persisty.access_control.authorization import Authorization, ROOT
from persisty.context import get_default_persisty_context, PersistyContext
from persisty.errors import PersistyError
from persisty.storage.storage_meta import StorageMeta
from persisty.strawberry.storage_schema_factory import StorageSchemaFactory


def new_schema_from_storage(
    authorization: Authorization = ROOT,
    persisty_context: Optional[PersistyContext] = None,
):
    if persisty_context is None:
        persisty_context = get_default_persisty_context()
    meta_storage = persisty_context.get_meta_storage(authorization)
    query_params: Dict = {}
    mutation_params: Dict = {}

    storage_meta_list = list(meta_storage.search_all())
    if not storage_meta_list:
        raise PersistyError("No storage detected in context!")

    marshaller = persisty_context.schema_context.marshaller_context.get_marshaller(
        StorageMeta
    )
    for storage_meta in meta_storage.search_all():
        storage_meta = marshaller.load(storage_meta)
        factory = StorageSchemaFactory(persisty_context, storage_meta)
        factory.add_to_schema(query_params, mutation_params)

    query_params["__annotations__"] = {f.name: f.type for f in query_params.values()}
    queries = strawberry.type(type("Query", (), query_params))

    mutation_params["__annotations__"] = {
        f.name: f.type for f in mutation_params.values()
    }
    mutations = strawberry.type(type("Mutation", (), mutation_params))

    schema = strawberry.Schema(queries, mutations)
    return schema
