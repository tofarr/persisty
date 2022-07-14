from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from marshy import get_default_context

from persisty.edit import Edit
from persisty.entity.entity_abc import EntityABC
from persisty.server.wsgi import start_server
from persisty.storage.in_mem.in_mem_storage import in_mem_storage
from persisty.storage.storage_context_abc import get_default_storage_context
from persisty.storage.wrappers.logging_storage import LoggingStorage
from persisty.storage.wrappers.schema_validated_storage import schema_validated_storage
from persisty.storage.wrappers.timestamped_storage import with_timestamps
from persisty.storage.wrappers.ttl_cache_storage import TTLCacheStorage
from tests.fixtures.item_types import Band, Member


@dataclass
class Person:
    name: str
    id: Optional[str] = None
    email: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class PersonEntity(Person, EntityABC):
    pass


if __name__ == '__main__':
    # Configure marshalling...
    marshy_context = get_default_context()

    # Configure storages...
    storage_context = get_default_storage_context()
    storage_context.register_storage(
        schema_validated_storage(
            TTLCacheStorage(
                LoggingStorage(
                    in_mem_storage(Band)
                )
            )
        )
    )
    storage_context.register_storage(
        schema_validated_storage(
            LoggingStorage(
                in_mem_storage(Member)
            )
        )
    )

    storage_context.register_storage(
        schema_validated_storage(
            with_timestamps(
                TTLCacheStorage(
                    LoggingStorage(
                        in_mem_storage(Person)
                    )
                )
            )
        )
    )
    persisty_context.register_entity(BandEntity)
    persisty_context.register_entity(MemberEntity)
    persisty_context.register_entity(PersonEntity)

    from tests.old.fixtures.data import setup_bands

    setup_bands(persisty_context.get_storage(Band))
    from tests.old.fixtures.data import setup_members

    setup_members(persisty_context.get_storage(Member))

    persisty_context.get_storage(Person).edit_all(
        Edit(EditType.CREATE, None, Person(member.member_name))
        for member in persisty_context.get_storage(Member).search()
    )

    # Start the server
    start_server()
