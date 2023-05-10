import inspect

from moto import mock_dynamodb


def mock_dynamodb_with_super(cls):
    """
    Create a new subclass for the class given where every method from the class or superclasses with a name stating
    with "test" is wrapped by the mock_dynamodb decorator
    """
    properties = {"run": mock_dynamodb(cls.run)}
    subclass = type(cls.__name__, (cls,), properties)
    return subclass
