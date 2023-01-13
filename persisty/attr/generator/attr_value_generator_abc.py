from abc import abstractmethod, ABC


class AttrValueGeneratorABC(ABC):
    @abstractmethod
    def transform(self, value):
        """Transform and return a value."""
