from abc import ABC


class SingletonABC(ABC):
    """Abstract singleton implementation"""

    def __new__(cls):
        instance = getattr(cls, "__instance", None)
        if instance is None:
            instance = object.__new__(cls)
            setattr(cls, "__instance", instance)
        return instance

    def __repr__(self):
        return self.__class__.__name__
