import importlib
import os

from persisty.persisty_context import PersistyContext

_default_context = None
PERSISTY_CONTEXT = 'PERSISTY_CONTEXT'


def get_default_persisty_context() -> PersistyContext:
    global _default_context
    if not _default_context:
        # Set up the default_context based on an environment variable
        import_name = os.environ.get(PERSISTY_CONTEXT, 'app.persisty_context')
        import_path = import_name.split('.')
        import_module = '.'.join(import_path[:-1])
        imported_module = importlib.import_module(import_module)
        persisty_context_fn = getattr(imported_module, import_path[-1])
        _default_context = persisty_context_fn()
    return _default_context
