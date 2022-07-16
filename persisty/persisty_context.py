import importlib
import pkgutil
from typing import Dict

from dataclasses import dataclass, field

from persisty.errors import PersistyError
from persisty.obj_storage.obj_storage import ObjStorage
from persisty.obj_storage.obj_storage_abc import ObjStorageABC
from persisty.storage.storage_abc import StorageABC


@dataclass
class PersistyContext:
    root_storage: ObjStorageABC[StorageABC] = None  # Root storage
    obj_storage: Dict[str, ObjStorage] = field(default_factory=dict)  # Programmatically defined storage


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
        if hasattr(module, "create_context"):
            context = PersistyContext(root_storage=getattr(module, "create_root_storage")())
            _configure_context(context, modules)
            return context
    raise PersistyError('no_context_config_found')


def _configure_context(context: PersistyContext, modules):
    modules.sort(key=lambda m: m.priority)
    for module in modules:
        if hasattr(module, "configure_context"):
            getattr(module, "configure_context")(context)
