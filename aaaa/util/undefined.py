from servey.util.singleton_abc import SingletonABC


class Undefined(SingletonABC):
    """
    Explicit value to be used for Undefined values. (Similar to Dataclasses MISSING but will not trigger an error on
    init of said dataclasses)

    For example, when a create / update is done but no value was specified, it just means we want these left alone, not
    that the update is Bad (Distinct from None, which can be a valid value for the stored)
    """

    def __bool__(self):
        return False  # Follow the javascript convention of making Undefined 'Falsy'

    def __eq__(self, other):
        return other is UNDEFINED

    def __repr__(self):
        return "undefined"

    def __hash__(self):
        return -1


UNDEFINED = Undefined()
