from abc import abstractmethod

from persisty.security.authorization import Authorization


class FieldAccessControlABC:

    @abstractmethod
    def is_readable(self, authorization: Authorization):
        """ Determine if information about this resource is available """

    @abstractmethod
    def is_writable(self, authorization: Authorization) -> bool:
        """ Determine if resource is creatable """
