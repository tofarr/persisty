from abc import abstractmethod, ABC
from typing import Optional

from marshy.factory.impl_marshaller_factory import get_impls
from servey.security.authorization import Authorization

from persisty.store.store_abc import StoreABC
from persisty.store_meta import get_meta
from persisty_dynamic.dynamic_store_meta import DynamicStoreMeta


class DynamicStoreABC(StoreABC[DynamicStoreMeta], ABC):
    """
    The standard persistence workflow is to have storage defined before deploying an app.
    Dynamic stores break this mold. They are not typically controlled by migrations, and can appear
    or disappear at any point as the application is run.

    An example use case: A user uploads a CSV which is imported into a data store
    and then used for analysis in a manner similar to other data stores. The user can browse the the
    CSVs they own, share data with others, and supply it to other APIs by means of REST.
    """

    def get_meta(self):
        return get_meta(DynamicStoreMeta)

    @abstractmethod
    def get_store(
        self, name: str, authorization: Optional[Authorization]
    ) -> Optional[StoreABC]:
        """
        Get the store with the name given
        """


class DynamicStoreFactoryABC(ABC):
    def create(self, store: StoreABC[DynamicStoreMeta]) -> Optional[DynamicStoreABC]:
        """Create a store"""


def get_dynamic_store() -> DynamicStoreABC:
    for factory in get_impls(DynamicStoreFactoryABC):
        dynamic_store = factory().create()
        if dynamic_store:
            return dynamic_store
    raise ValueError("no_suitable_implementation")
