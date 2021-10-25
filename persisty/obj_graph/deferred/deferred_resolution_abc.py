from abc import ABC, abstractmethod


class DeferredResolutionABC(ABC):

    @abstractmethod
    def resolve(self):
        """ Resolve this deferred action"""
