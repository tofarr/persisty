import importlib
import pkgutil

from persisty.context.persisty_context import PersistyContext
from persisty.errors import PersistyError

_default_context = None
CONFIG_MODULE_PREFIX = "persisty_config_"


def get_default_persisty_context() -> PersistyContext:
    global _default_context
    if not _default_context:
        _default_context = new_default_persisty_context()
    return _default_context


def new_default_persisty_context() -> PersistyContext:
    # Set up context based on naming convention
    module_info = (
        m for m in pkgutil.iter_modules() if m.name.startswith(CONFIG_MODULE_PREFIX)
    )
    modules = [importlib.import_module(m.name) for m in module_info]
    modules.sort(key=lambda m: m.priority, reverse=True)
    for module in modules:
        if hasattr(module, "create_meta_storage"):
            meta_storage = module.create_meta_storage()
            context = PersistyContext(meta_storage)
            _configure_context(context, modules)
            return context
    raise PersistyError("no_context_config_found")


def _configure_context(context: PersistyContext, modules):
    modules.sort(key=lambda m: m.priority)
    for module in modules:
        if hasattr(module, "configure_context"):
            getattr(module, "configure_context")(context)
