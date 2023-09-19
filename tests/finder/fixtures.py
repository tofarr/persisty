from uuid import UUID

from persisty.stored import stored


@stored
class Message:
    id: UUID
    owner: str
    text: str
