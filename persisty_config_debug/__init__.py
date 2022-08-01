"""
This is really just here to test that things work as expected. Delete when done
"""
from datetime import datetime
from typing import Optional, Union, Type
from uuid import UUID

from schemey.schema import str_schema
from schemey.string_format import StringFormat

from persisty.access_control.authorization import ROOT
from persisty.context import PersistyContext
from persisty.entity import entity_context
from persisty.entity.entity import Entity
from persisty.obj_storage.attr import Attr
from persisty.obj_storage.stored import stored
from persisty.relation.belongs_to import BelongsTo


priority = 90


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
    from_user: User = BelongsTo(entity_name='User')
    to_user: User = BelongsTo(entity_name='User')


UserEntity: Optional[Union[Type[User], Type[Entity]]] = None
MessageEntity: Optional[Union[Type[Message], Type[Entity]]] = None


def configure_entities(persisty_context: PersistyContext):
    global UserEntity, MessageEntity
    UserEntity = entity_context.create_entity_type(User, persisty_context)
    MessageEntity = entity_context.create_entity_type(Message, persisty_context)


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


def configure_context(persisty_context: PersistyContext):
    configure_entities(persisty_context)
    seed_data()
