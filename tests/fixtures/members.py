from typing import Optional
from uuid import uuid4, UUID

from dataclasses import dataclass


@dataclass
class Member:
    id: UUID
    name: str
    dob: str
    band_code: str
    male: bool
    ordering: Optional[int] = None


MEMBERS = [
    dict(id=str(uuid4()), name='John Lennon', dob='1940-10-09', band_code='beatles', male=True, ordering=1),
    dict(id=str(uuid4()), name='Paul McCartney', dob='1942-06-18', band_code='beatles', male=True, ordering=1),
    dict(id=str(uuid4()), name='George Harrison', dob='1943-02-25', band_code='beatles', male=True, ordering=1),
    dict(id=str(uuid4()), name='Ringo Starr', dob='1940-07-07', band_code='beatles', male=True, ordering=1),
]

EXTRA_BEATLE = dict(id=str(uuid4()), name='Pete Best', dob='1941-11-24', band_code='beatles')

