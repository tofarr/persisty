from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from marshy import get_default_context

from persisty.edit import Edit
from old.persisty import EditType
from persisty.marshaller import register_all_marshallers
from persisty.obj_graph import EntityABC
from persisty.server.wsgi import start_server
from old.persisty.persisty_context import get_default_persisty_context

from old.persisty.storage.in_mem_storage import in_mem_storage
from old.persisty.storage.schema_storage import schema_storage
from old.persisty.storage import LoggingStorage
from old.persisty.storage.timestamp_storage import timestamp_storage
from old.persisty.storage.ttl_cache_storage import TTLCacheStorage
from tests.fixtures.items import Band, Member
from tests.fixtures.entities import BandEntity, MemberEntity


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
    register_all_marshallers(marshy_context)

    # Configure storages...
    persisty_context = get_default_persisty_context()
    persisty_context.register_storage(
        schema_storage(
            TTLCacheStorage(
                LoggingStorage(
                    in_mem_storage(Band)
                )
            )
        )
    )
    persisty_context.register_storage(
        schema_storage(
            LoggingStorage(
                in_mem_storage(Member)
            )
        )
    )

    persisty_context.register_storage(
        schema_storage(
            timestamp_storage(
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

    from tests.fixtures.data import setup_bands

    setup_bands(persisty_context.get_storage(Band))
    from tests.fixtures.data import setup_members

    setup_members(persisty_context.get_storage(Member))

    persisty_context.get_storage(Person).edit_all(
        Edit(EditType.CREATE, None, Person(member.member_name))
        for member in persisty_context.get_storage(Member).search()
    )

    # Start the server
    start_server()
