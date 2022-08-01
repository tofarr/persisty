from abc import ABC, abstractmethod


class RelationABC(ABC):
    """
    A relation represents something external to a stored item but related to it.
    """

    @abstractmethod
    def __set_name__(self, owner, name):
        """Called when a relation is set as a value for a @stored entity."""

    @abstractmethod
    def get_name(self) -> str:
        """Get the name of this relation"""

    @abstractmethod
    def to_property_descriptor(self):
        """Create a property descriptor for this relation"""
