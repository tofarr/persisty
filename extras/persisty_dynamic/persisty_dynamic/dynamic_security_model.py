from dataclasses import dataclass

from persisty_dynamic.dynamic_permission import DynamicPermission, DENIED


@dataclass
class DynamicSecurityModel:
    config_permission: DynamicPermission = DENIED
    create_permission: DynamicPermission = DENIED
    read_permission: DynamicPermission = DENIED
    update_permission: DynamicPermission = DENIED
    deleted_permission: DynamicPermission = DENIED
    search_permission: DynamicPermission = DENIED
