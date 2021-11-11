from abc import ABC, abstractmethod


class AccessControlABC(ABC):

    @abstractmethod
    @property
    def is_creatable(self) -> bool:
        """ Determine if resource is creatable """

    @abstractmethod
    @property
    def is_readable(self) -> bool:
        """ Determine if resource is readable """

    @abstractmethod
    @property
    def is_updatable(self) -> bool:
        """ Determine if resource is updatable """

    @abstractmethod
    @property
    def is_destroyable(self) -> bool:
        """ Determine if resource is destroyable """

    @abstractmethod
    @property
    def is_searchable(self) -> bool:
        """ Determine if resource is searchable """
