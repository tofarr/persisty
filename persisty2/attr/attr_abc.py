from abc import abstractmethod, ABC
from typing import Generic, TypeVar, Type, Optional

from schemey.schema_abc import SchemaABC

from persisty2.attr.attr_access_control_abc import AttrAccessControlABC
from persisty2.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty2.selections import Selections

A = TypeVar('A')
B = TypeVar('B')


class AttrABC(ABC, Generic[A, B]):

    @property
    @abstractmethod
    def name(self) -> str:
        """ Get the name of this attribute """

    @property
    @abstractmethod
    def type(self) -> Type[B]:
        """  Get the type for this attribute """

    @abstractmethod
    def __set_name__(self, owner, name):
        """ set the name and type for this attribute """

    @abstractmethod
    def __get__(self, owner_instance: A, owner_type: Type[A]) -> B:
        """ Get the value for this attribute """

    @abstractmethod
    def __set__(self, owner_instance: A, value: B):
        """ Set the value for this attribute"""

    @property
    @abstractmethod
    def schema(self) -> SchemaABC[B]:
        """ Get the name of this attribute """

    @property
    @abstractmethod
    def attr_access_control(self) -> AttrAccessControlABC:
        """ Get the name of this attribute """

    @abstractmethod
    def unresolve(self, owner_instance: A):
        """ Unresolve this attribute"""

    @abstractmethod
    def is_resolved(self, owner_instance: A) -> bool:
        """ Determine if this attribute is resolved """

    @abstractmethod
    def is_save_required(self, owner_instance: A) -> bool:
        """ Determine if this attribute requires a save (Has had a value set for it which differs from the server """

    @abstractmethod
    def resolve(self,
                owner_instance: A,
                selections: Optional[Selections] = None,
                deferred_resolutions: Optional[DeferredResolutionSet] = None):
        """ Resolve this attribute if it requires deferred resolution, and any attributes in the selection set given. """
