

class GeneratorMode(Enum):
    ALWAYS_FOR_CREATE = 'always_for_create'
    ALWAYS_FOR_UPDATE = 'always_for_update'
    ALWAYS_FOR_WRITE = 'always_for_write'
    OPTIONAL_FOR_CREATE = 'optional_for_create'
    OPTIONAL_FOR_UPDATE = 'optional_for_update'
    OPTIONAL_FOR_WRITE = 'optional_for_write'