from abc import ABC, abstractmethod
from typing import Dict, Type, Union, ForwardRef, Generic, TypeVar

from marshy.types import ExternalItemType

from persisty.attr.attr import Attr

T = TypeVar("T")


class LinkABC(ABC, Generic[T]):
    """
    A link represents something external to a stored item but related to it.
    """

    @abstractmethod
    def __set_name__(self, owner, name):
        """Called when a link is set as a value for a @stored entity."""

    @abstractmethod
    def get_name(self) -> str:
        """Get the name of this link"""

    def get_linked_type(self, forward_ref_ns: str) -> Union[ForwardRef, Type]:
        """Get the type for the linked object (May be an int, Optional[T], ResultSet[T], or something else"""

    def update_attrs(self, attrs_by_name: Dict[str, Attr]):
        """
        Update parameters to include anything required by this link that may be missing. (e.g. linked_id)
        """

    def update_json_schema(self, json_schema: ExternalItemType):
        """
        Update a schema with data from this link (Typically this is used as a custom extension to jsonschema to
        represent relational data.
        """

    def before_create(self, item: T):
        """Called before an item is created"""

    def after_create(self, item: T):
        """Called before an item is created"""

    def before_update(self, item: T, updates: T):
        """Called before an item is created"""

    def after_update(self, oldItem: T, newItem: T):
        """Called before an item is created"""

    def before_delete(self, item: T):
        """Called before an item is created"""

    def after_delete(self, item: T):
        """Called before an item is created"""
