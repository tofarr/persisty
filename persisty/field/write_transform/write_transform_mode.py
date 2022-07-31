from enum import Enum


class WriteTransformMode(Enum):
    GENERATED = "generated"
    OPTIONAL = "optional"
    SPECIFIED = "specified"
