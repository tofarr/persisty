from abc import ABC, abstractmethod

from persisty.access_control.authorization import Authorization


class AccessControlABC(ABC):

    @abstractmethod
    def is_meta_accessible(self, authorization: Authorization):
        """ Determine if information about this resource is available """

    @abstractmethod
    def is_creatable(self, authorization: Authorization) -> bool:
        """ Determine if resource is creatable """

    @abstractmethod
    def is_readable(self, authorization: Authorization) -> bool:
        """ Determine if resource is readable """

    @abstractmethod
    def is_updatable(self, authorization: Authorization) -> bool:
        """ Determine if resource is updatable """

    @abstractmethod
    def is_deletable(self, authorization: Authorization) -> bool:
        """ Determine if resource is destroyable """

    @abstractmethod
    def is_searchable(self, authorization: Authorization) -> bool:
        """ Determine if resource is searchable """
