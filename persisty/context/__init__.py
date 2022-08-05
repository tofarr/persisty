import importlib
import pkgutil

from persisty.context.persisty_context import PersistyContext
from persisty.impl.mem.mem_meta_storage import MemMetaStorage

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
    modules.sort(key=lambda m: m.priority)
    persisty_context_ = PersistyContext(MemMetaStorage())  # The idea is that configs override this...
    for module in modules:
        # noinspection PyUnresolvedReferences
        persisty_context_ = module.configure_context(persisty_context_)
    return persisty_context_
