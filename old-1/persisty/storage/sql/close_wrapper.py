

class CloseWrapper:
    """
    DB-API does not seem to implement the __enter__ / __exit__ protocol which would allow it to use with clauses.
    This rectifies that
    """

    def __init__(self, delegate):
        self._delegate = delegate

    def __getattr__(self, key):
        attr = getattr(self._delegate, key)
        return attr

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._delegate.close()
