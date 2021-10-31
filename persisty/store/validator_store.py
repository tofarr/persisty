from dataclasses import dataclass
from typing import Iterator

from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC, T
from persisty.validate.validator_abc import ValidatorABC


@dataclass(frozen=True)
class ValidatorStore(WrapperStoreABC[T]):
    """
    Wrapper which covers another and includes a search filter. Creates or update not matching the search filter
    fail outright. Effectively creates a partial view of another store (Useful for enforcing security constraints)
    """
    wrapped_store: StoreABC[T]
    validator: ValidatorABC[T]

    @property
    def store(self) -> StoreABC[T]:
        return self.wrapped_store

    def create(self, item: T) -> str:
        self._validate_item(item)
        return self.store.create(item)

    def update(self, item: T) -> T:
        self._validate_item(item)
        return self.store.update(item)

    def edit_all(self, edits: Iterator[Edit[T]]):
        """ Perform a bulk edit for items in this store. This action is not typically atomic. """
        edits = self._validate_edits(edits)
        self.store.edit_all(edits)

    def _validate_edits(self, edits: Iterator[Edit[T]]) -> Iterator[Edit[T]]:
        for edit in edits:
            if edit.edit_type in [EditType.CREATE, EditType.UPDATE]:
                self._validate_item(edit.item)
                yield edit

    def _validate_item(self, item: T):
        error = next(self.validator.get_validation_errors(item), None)
        if error:
            raise error
