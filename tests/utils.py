import inspect

from moto import mock_dynamodb


def mock_dynamodb_with_super(cls):
    """
    Create a new subclass for the class given where every method from the class or superclasses with a name stating
    with "test" is wrapped by the mock_dynamodb decorator
    """
    params = {}
    for c in reversed(inspect.getmro(cls)):
        for k, v in c.__dict__.items():
            if k.startswith('test') and hasattr(v, '__call__'):
                params[k] = mock_dynamodb(v)
    subclass = type(cls.__name__, (cls,), params)
    return subclass
