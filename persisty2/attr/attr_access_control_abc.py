from abc import ABC, abstractmethod

from persisty2.attr.attr_mode import AttrMode


class AttrAccessControlABC(ABC):

    @abstractmethod
    @property
    def create_mode(self) -> AttrMode:
        """ Determine if resource is creatable """

    @abstractmethod
    @property
    def update_mode(self) -> AttrMode:
        """ Determine if resource is readable """

    @abstractmethod
    @property
    def read_mode(self) -> AttrMode:
        """ Determine if resource is updatable """

    @abstractmethod
    @property
    def search_mode(self) -> AttrMode:
        """ Determine if resource is destroyable """
