from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from marshy import get_default_context

from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.marshaller import register_all_marshallers
from persisty.obj_graph.entity_abc import EntityABC
from persisty.server.wsgi import start_server
from persisty.persisty_context import get_default_persisty_context

from persisty.store.in_mem_store import in_mem_store
from persisty.store.schema_store import schema_store
from persisty.store.logging_store import LoggingStore
from persisty.store.timestamp_store import timestamp_store
from persisty.store.ttl_cache_store import TTLCacheStore
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

    # Configure stores...
    persisty_context = get_default_persisty_context()
    persisty_context.register_store(
        schema_store(
            TTLCacheStore(
                LoggingStore(
                    in_mem_store(Band)
                )
            )
        )
    )
    persisty_context.register_store(
        schema_store(
            LoggingStore(
                in_mem_store(Member)
            )
        )
    )

    persisty_context.register_store(
        schema_store(
            timestamp_store(
                TTLCacheStore(
                    LoggingStore(
                        in_mem_store(Person)
                    )
                )
            )
        )
    )
    persisty_context.register_entity(BandEntity)
    persisty_context.register_entity(MemberEntity)
    persisty_context.register_entity(PersonEntity)

    from tests.fixtures.data import setup_bands

    setup_bands(persisty_context.get_store(Band))
    from tests.fixtures.data import setup_members

    setup_members(persisty_context.get_store(Member))

    persisty_context.get_store(Person).edit_all(
        Edit(EditType.CREATE, None, Person(member.member_name))
        for member in persisty_context.get_store(Member).search()
    )

    # Start the server
    start_server()
