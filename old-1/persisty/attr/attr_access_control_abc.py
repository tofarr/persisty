from abc import ABC, abstractmethod

from persisty.attr.attr_mode import AttrMode


class AttrAccessControlABC(ABC):

    @property
    @abstractmethod
    def create_mode(self) -> AttrMode:
        """ Determine if resource is creatable """

    @property
    @abstractmethod
    def update_mode(self) -> AttrMode:
        """ Determine if resource is readable """

    @property
    @abstractmethod
    def read_mode(self) -> AttrMode:
        """ Determine if resource is updatable """

    @property
    @abstractmethod
    def search_mode(self) -> AttrMode:
        """ Determine if resource is destroyable """
