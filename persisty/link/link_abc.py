from abc import ABC, abstractmethod

from marshy.types import ExternalItemType
from schemey import Schema


class LinkABC(ABC):
    """
    A link represents something external to a stored item but related to it.
    """

    @abstractmethod
    def __set_name__(self, owner, name):
        """Called when a link is set as a value for a @stored entity."""

    @abstractmethod
    def get_name(self) -> str:
        """Get the name of this link"""

    @abstractmethod
    def to_property_descriptor(self):
        """Create a property descriptor for this link"""

    def update_json_schema(self, json_schema: ExternalItemType):
        """
        Update a schema with data from this link (Typically this is used as a custom extension to jsonschema to
        represent relational data.
        """
