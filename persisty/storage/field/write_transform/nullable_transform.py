from marshy import ExternalType

from persisty.field.write_transform.write_transform_abc import WriteTransformABC
from persisty.field.write_transform.write_transform_mode import WriteTransformMode
from persisty.util.singleton_abc import SingletonABC
from persisty.util.undefined import UNDEFINED


class NullableTransform(SingletonABC, WriteTransformABC):

    @property
    def generator_mode(self) -> WriteTransformMode:
        return WriteTransformMode.OPTIONAL_FOR_CREATE

    def transform(self, specified_value: ExternalType, is_update: bool = False) -> ExternalType:
        if not is_update and specified_value is UNDEFINED:
            return None
        return specified_value


NULL_GENERATOR = NullGenerator()
