from abc import abstractmethod

from persisty.security.authorization import Authorization


class FieldAccessControlABC:

    @abstractmethod
    def is_readable(self, authorization: Authorization):
        """ Determine if the field is readable """

    @abstractmethod
    def is_writable(self, authorization: Authorization) -> bool:
        """ Determine if the field is writable """
