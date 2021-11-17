from abc import ABC, abstractmethod


class AccessControlABC(ABC):

    @property
    @abstractmethod
    def is_creatable(self) -> bool:
        """ Determine if resource is creatable """

    @property
    @abstractmethod
    def is_readable(self) -> bool:
        """ Determine if resource is readable """

    @property
    @abstractmethod
    def is_updatable(self) -> bool:
        """ Determine if resource is updatable """

    @property
    @abstractmethod
    def is_destroyable(self) -> bool:
        """ Determine if resource is destroyable """

    @property
    @abstractmethod
    def is_searchable(self) -> bool:
        """ Determine if resource is searchable """
