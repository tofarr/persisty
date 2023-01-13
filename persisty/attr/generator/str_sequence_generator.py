from dataclasses import dataclass

from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC


@dataclass
class StrSequenceGenerator(AttrValueGeneratorABC):
    """
    Sequence Id Generator. Note: There could be a security concern with making this optional for create operations - for
    filtered storage, it could open a way for attackers to check if an id exists. (By trying to create it)
    Typically sql based keys will generate this value in the database rather than relying on a client.
    """

    format = "{value}"
    always: bool = True
    value: int = 1
    step: int = 1

    def transform(self, value):
        if self.always or not value:
            value = self.format.format(value=self.value)
            self.value += self.step
        return value
