"""
This is really just here to test that things work as expected. Delete when done
"""
from __future__ import annotations
from datetime import datetime
from typing import Optional, Union, Type
from uuid import UUID

from schemey.schema import str_schema
from schemey.string_format import StringFormat

from persisty.access_control.authorization import ROOT
from persisty.context import PersistyContext
from persisty.entity import entity_context
from persisty.entity.entity import Entity
from persisty.impl.mem.mem_storage_schema import MemStorageSchema
from persisty.obj_storage.attr import Attr
from persisty.obj_storage.stored import stored
from persisty.link.belongs_to import BelongsTo
from persisty.link.has_count import HasCount
from persisty.link.has_many import HasMany
from persisty.storage.result_set import ResultSet

priority = 100


@stored
class User:
    """A user of the system"""

    id: UUID
    email: str = Attr(schema=str_schema(str_format=StringFormat.EMAIL, max_length=255))
    created_at: datetime
    updated_at: datetime


@stored
class Message:
    """A message sent between users"""

    id: UUID
    text: str
    from_user_id: UUID
    to_user_id: UUID
    created_at: datetime
    updated_at: datetime
    from_user: User = BelongsTo()
    to_user: User = BelongsTo()


@stored
class Node:
    """Testing self referencing data stores"""

    id: UUID
    title: str
    parent_id: Optional[UUID]
    created_at: datetime
    updated_at: datetime
    parent: Optional[Node] = BelongsTo(storage_name="node")
    child_count: int = HasCount(storage_name="node", id_field_name="parent_id")
    children: ResultSet[Node] = HasMany(storage_name="node", id_field_name="parent_id")


# Entities will need to be set up later, because they rely on the persisty_context...
UserEntity: Optional[Union[Type[User], Type[Entity]]] = None
MessageEntity: Optional[Union[Type[Message], Type[Entity]]] = None
NodeEntity: Optional[Union[Type[Node], Type[Entity]]] = None


def configure_entities(persisty_context: PersistyContext):
    # Configure entities
    global UserEntity, MessageEntity, NodeEntity
    UserEntity = entity_context.create_entity_type(User, persisty_context)
    MessageEntity = entity_context.create_entity_type(Message, persisty_context)
    NodeEntity = entity_context.create_entity_type(Node, persisty_context)


def seed_data():
    UserEntity(
        ROOT, id=UUID("00000000-0000-0000-0000-000000000001"), email="alice@test.com"
    ).create()
    UserEntity(
        ROOT, id=UUID("00000000-0000-0000-0000-000000000002"), email="bob@test.com"
    ).create()
    MessageEntity(
        ROOT,
        text="Welcome to the system!",
        from_user_id=UUID("00000000-0000-0000-0000-000000000001"),
        to_user_id=UUID("00000000-0000-0000-0000-000000000002"),
    ).create()
    MessageEntity(
        ROOT,
        text="Thank you for the invitation!",
        from_user_id=UUID("00000000-0000-0000-0000-000000000002"),
        to_user_id=UUID("00000000-0000-0000-0000-000000000001"),
    ).create()
    NodeEntity(
        ROOT, id=UUID("00000000-0000-0000-0000-00000000000a"), title="A"
    ).create()
    NodeEntity(
        ROOT,
        id=UUID("00000000-0000-0000-0000-0000000000ab"),
        title="AB",
        parent_id=UUID("00000000-0000-0000-0000-00000000000a"),
    ).create()
    NodeEntity(
        ROOT,
        id=UUID("00000000-0000-0000-0000-0000000000ac"),
        title="AC",
        parent_id=UUID("00000000-0000-0000-0000-00000000000a"),
    ).create()
    NodeEntity(
        ROOT,
        id=UUID("00000000-0000-0000-0000-000000000abd"),
        title="D",
        parent_id=UUID("00000000-0000-0000-0000-0000000000ab"),
    ).create()
    NodeEntity(
        ROOT,
        id=UUID("00000000-0000-0000-0000-000000000abe"),
        title="E",
        parent_id=UUID("00000000-0000-0000-0000-0000000000ab"),
    ).create()


def configure_context(persisty_context: PersistyContext) -> PersistyContext:
    persisty_context.register_storage_schema(MemStorageSchema())
    configure_entities(persisty_context)
    seed_data()
    return persisty_context
