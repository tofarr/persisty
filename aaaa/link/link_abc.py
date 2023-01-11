from abc import ABC, abstractmethod
from typing import Dict, List

from marshy.types import ExternalItemType

from aaaa.attr.attr import Attr


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
    def to_action_fn(self, owner_name: str):
        """ Create an action function for this link (When in entity mode, this is wrapped by a property descriptor) """

    def update_attrs(self, attrs: Dict[str, Attr]):
        """
        Update parameters
        """

    def update_json_schema(self, json_schema: ExternalItemType):
        """
        Update a schema with data from this link (Typically this is used as a custom extension to jsonschema to
        represent relational data.
        """
