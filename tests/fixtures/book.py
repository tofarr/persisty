from typing import ForwardRef

from marshy import dump

from persisty.link.belongs_to import BelongsTo
from persisty.stored import stored


@stored(batch_size=10)
class Book:
    """Item linking a string representing a number with an integer value. Also has a uuid, and timestamps."""
    id: int
    title: str
    author = BelongsTo(linked_store_type=ForwardRef('tests.fixtures.author.Author'))


BOOKS = [
    Book(1, 'The Three Musketeers', "1"),
    Book(2, 'The Count of Monte Cristo', "1"),
    Book(3, 'Frankenstein', "2"),
    Book(4, 'The Adventures of Sherlock Holmes', "3"),
    Book(5, 'The Memoirs of Sherlock Holmes', "3"),
    Book(6, 'The Hound of the Baskervilles', "3"),
    Book(7, 'The Return of Sherlock Holmes', "3"),
    Book(8, 'The Lost World', "3"),
]

BOOK_DICTS = [dump(n) for n in BOOKS]
