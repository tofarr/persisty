from abc import abstractmethod, ABC


class MutatorMode:
    ON_CREATE = 'on_create'
    ON_UPDATE = 'on_update'
    ON_WRITE = 'on_write'


class MutatorABC(ABC):

    @abstractmethod
    @property
    def field_name(self):
        """ Get the name of the field for which a value will be created """

    @abstractmethod
    @property
    def mutator_mode(self):
        """ Get the mode for mutations """

    @abstractmethod
    def generate_value(self):
        """ Generate a new value for this field """