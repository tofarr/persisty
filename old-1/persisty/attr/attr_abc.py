from abc import abstractmethod, ABC
from typing import Generic, TypeVar, Type, Optional, Iterator

from schemey.schema_abc import SchemaABC

from persisty.attr.attr_access_control_abc import AttrAccessControlABC
from persisty.cache_header import CacheHeader
from persisty.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.entity.selections import Selections

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
        """ Resolve this attribute if it requires deferred resolution, and any attributes in the selection set. """

    def get_cache_headers(self, owner_instance: A, selections: Selections) -> Iterator[CacheHeader]:
        """ Get a cache header for the resolved value """
        pass

    def before_create(self, owner_instance: A):
        """ Hook invoked immediately before an entity is created """
        pass

    def after_create(self, owner_instance: A):
        """ Hook invoked immediately before an entity is created """
        pass

    def before_update(self, owner_instance: A):
        """ Hook invoked immediately before an entity is updated """
        pass

    def after_update(self, owner_instance: A):
        """ Hook invoked immediately after an entity is updated """
        pass

    def before_destroy(self, owner_instance: A):
        """ Hook invoked immediately before an entity is destroyed """
        pass

    def after_destroy(self, owner_instance: A):
        """ Hook invoked immediately after an entity is destroyed """
        pass
