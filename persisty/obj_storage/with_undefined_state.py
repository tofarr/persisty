from dataclasses import dataclass, fields, MISSING

from persisty.util import UNDEFINED


def with_undefined_state(cls, name: str = None):
    """
    Decorator inspired by dataclasses, making sure objects are compatible with being created with an undefined
    state for use by persisty - not frozen and with UNDEFINED defaults where required.

    The new class does not extend the old one, in case the old one is Frozen
    """

    def wrapper(cls_):
        new_dict = {"__annotations__": {**cls_.__dict__["__annotations__"]}}
        for field in fields(cls_):
            if field.default is MISSING and field.default_factory is MISSING:
                new_dict[field.name] = UNDEFINED
            else:
                new_dict[field.name] = field
        wrapped = type(name or cls_.__name__, tuple(), new_dict)
        wrapped = dataclass(wrapped)
        return wrapped

    return wrapper if cls is None else wrapper(cls)
