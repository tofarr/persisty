import dataclasses
import os
from pathlib import Path
from typing import Dict

from persisty.impl.default_store import DefaultStore
from persisty.store_meta import get_meta
from persisty_data.chunk import Chunk
from persisty_data.chunk_data_store import ChunkDataStore
from persisty_data.content_meta import ContentMeta
from persisty_data.data_store_abc import DataStoreABC
from persisty_data.directory_data_store import DirectoryDataStore


def default_data_store(
    name: str, target: Dict, max_item_size: int = 1024 * 1024 * 50
) -> DataStoreABC:
    """
    Create a default data store. If there is an PERSISTY_DATA_S3_BUCKET value environment variable, then use it.
    If there is a PERSISTY_DATA_DIRECTORY in the environment, then we use that
    Otherwise we use a chunk store, and add stores for content meta and chunks to the target.
    """
    persisty_data_s3_bucket = os.environ.get("PERSISTY_DATA_S3_BUCKET")
    if persisty_data_s3_bucket:
        from persisty_data.s3_data_store import S3DataStore

        return S3DataStore(persisty_data_s3_bucket)
    persisty_data_directory = os.environ.get("PERSISTY_DATA_DIRECTORY")
    if persisty_data_directory:
        return DirectoryDataStore(name=name, directory=Path(persisty_data_directory))
    content_meta_store = DefaultStore(
        dataclasses.replace(get_meta(ContentMeta), name=name + "_content_meta")
    )
    target[content_meta_store.get_meta().name] = content_meta_store
    chunk_store = DefaultStore(
        dataclasses.replace(get_meta(Chunk), name=name + "_chunk")
    )
    target[chunk_store.get_meta().name] = chunk_store
    chunk_data_store = ChunkDataStore(
        name=name,
        content_meta_store=content_meta_store,
        chunk_store=chunk_store,
        max_item_size=max_item_size,
    )
    return chunk_data_store
