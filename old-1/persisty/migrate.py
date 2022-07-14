import importlib
from datetime import datetime
from importlib import resources
from pathlib import Path
import pkgutil
import sys

from persisty.migration.migration_result import CompletedMigration
from persisty.storage.storage_context import StorageContext, get_default_storage_context
from persisty.storage.storage_meta import storage_meta_from_dataclass
from persisty.util import get_logger

logger = get_logger(__name__)
MIGRATE_MODULE_PREFIX = 'persisty_migrations'
TEMPLATE_MODULE = 'persisty.migration'
TEMPLATE_FILE = 'template.py'


def generate(name: str = None):
    """
    Generate a new migration. e.g.: `python -m persisty.migrate generate state add_band`
    """
    group_path = Path(MIGRATE_MODULE_PREFIX)
    if not group_path.exists():
        group_path.mkdir()
    init_path = Path(f'{group_path}/__init__.py')
    if not init_path.exists():
        init_path.touch()
    migration_name = datetime.now().strftime('%y%m%d%H%M%S')
    if name:
        migration_name += '_'+_sanitize_name(name)
    migration_name += '.py'
    migration_path = group_path.joinpath(migration_name)
    template = resources.read_text(TEMPLATE_MODULE, TEMPLATE_FILE)
    with open(migration_path, "w") as text_file:
        text_file.write(template)


def _sanitize_name(name: str) -> str:
    return name.lower().replace(' ', '_')


def get_status(storage_context: StorageContext):
    """
    Get the pending migrations
    """
    init_for_migrations(storage_context)
    for migration in _load_migrations():
        # noinspection PyUnresolvedReferences
        status = 'DONE' if migration.is_completed(storage_context) else 'TODO'
        logger.info(status + '\t:\t' + migration.__name__)


def migrate(storage_context: StorageContext, steps: int = 1000):
    """
    e.g.: `python -m persisty.migrate migrate`
    """
    init_for_migrations(storage_context)
    for migration in _load_migrations():
        # noinspection PyUnresolvedReferences
        if is_completed(storage_context, migration.__name__):
            logger.debug(f'skipping:{migration.__name__}')
        else:
            logger.info(f'running:{migration.__name__}')
            # noinspection PyUnresolvedReferences
            migration.migrate(storage_context)
            steps -= 1
            if steps == 0:
                return


def rollback(storage_context: StorageContext, steps: int = 1):
    init_for_migrations(storage_context)
    for migration in _load_migrations():
        # noinspection PyUnresolvedReferences
        if not is_completed(storage_context, migration.__name__):
            logger.debug(f'skipping:{migration.__name__}')
        else:
            logger.info(f'rolling_back:{migration.__name__}')
            # noinspection PyUnresolvedReferences
            migration.migrate(storage_context)
            steps -= 1
            if steps == 0:
                return


def _load_migrations():
    group_path = MIGRATE_MODULE_PREFIX
    module_info = pkgutil.iter_modules([group_path], group_path + '.')
    modules = [importlib.import_module(m.name) for m in module_info]
    modules.sort(key=lambda m: m.__name__)
    return modules


def init_for_migrations(storage_context: StorageContext):
    dynamic_storage = storage_context.dynamic_storage
    meta = dynamic_storage.read(CompletedMigration.__name__)
    if meta is None:
        meta = storage_meta_from_dataclass(CompletedMigration)
        dynamic_storage.create(meta)


def is_completed(storage_context: StorageContext, migration_name: str) -> bool:
    storage = storage_context.get_storage(CompletedMigration.__name__)
    completed_migration = storage.read(migration_name)
    return completed_migration is not None


def main():
    args = sys.argv
    op = args[1]
    if op == 'generate':
        name = args[2] if len(args) > 2 else None
        generate(name)
    elif op == 'get_status':
        get_status(get_default_storage_context())
    elif op == 'migrate':
        steps = args[2] if len(args) > 2 else 1000
        migrate(get_default_storage_context(), steps)
    elif op == 'rollback':
        steps = args[2] if len(args) > 2 else 1
        rollback(get_default_storage_context(), steps)
    else:
        raise ValueError(f'unknown_command:{args[1]}')


if __name__ == '__main__':
    main()
