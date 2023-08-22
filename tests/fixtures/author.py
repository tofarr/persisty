from typing import ForwardRef

from marshy import dump

from persisty.link.has_many import HasMany
from persisty.stored import stored


@stored(batch_size=10)
class Author:
    """Item linking a string representing a number with an integer value. Also has a uuid, and timestamps."""
    id: int
    full_name: str
    books = HasMany(linked_store_type=ForwardRef('tests.fixtures.book.Book'))


AUTHORS = [
    Author(1, 'Alexandre Dumas'),
    Author(2, 'Mary Shelly'),
    Author(3, 'Arthur Conan Doyle'),
]

AUTHOR_DICTS = [dump(n) for n in AUTHORS]
