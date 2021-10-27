import importlib
import os

from persisty.persisty_context import PersistyContext

_persisty_context = None
PERSISTY_CONTEXT = 'PERSISTY_CONTEXT'


def get_persisty_context() -> PersistyContext:
    global _persisty_context
    if not _persisty_context:
        # Set up the persisty_context based on an environment variable
        import_name = os.environ.get(PERSISTY_CONTEXT)
        if import_name:
            import_path = import_name.split('.')
            import_module = '.'.join(import_path[:-1])
            imported_module = importlib.import_module(import_module)
            persisty_context_fn = getattr(imported_module, import_path[-1])
            _persisty_context = persisty_context_fn()
        else:
            _persisty_context = PersistyContext()
    return _persisty_context
