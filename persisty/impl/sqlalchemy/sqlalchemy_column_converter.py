import inspect
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Iterator

from marshy.factory.optional_marshaller_factory import get_optional_type
from marshy.types import ExternalItemType
from sqlalchemy import (
    Column,
    Integer,
    Float,
    String,
    Text,
    DateTime,
    Boolean,
    Enum as SqlalchemyEnum,
    LargeBinary,
)

# noinspection PyPep8Naming
from sqlalchemy.dialects.postgresql import UUID as PostgresUuid, JSON as PostgresJson

# noinspection PyPep8Naming
from sqlalchemy.dialects.mysql import JSON as MysqlJson

# noinspection PyPep8Naming
from sqlalchemy.dialects.mssql import JSON as MssqlJson

from persisty.attr.attr import Attr
from persisty.key_config.key_config_abc import KeyConfigABC

POSTGRES = "postgres"
MYSQL = "mysql"
MSSQL = "mssql"


@dataclass
class SqlalchemyColumnConverter:
    key_config: KeyConfigABC
    dialect: str

    def create_column(self, attr: Attr) -> Column:
        fn_name = f"_create_{attr.attr_type.value}"
        args = [attr.name, getattr(self, fn_name)(attr)]
        kwargs = {
            "nullable": bool(get_optional_type(attr.schema.python_type)),
        }
        if attr.name in self.key_config.get_key_attrs():
            kwargs["primary_key"] = True
        column = Column(*args, **kwargs)
        return column

    # noinspection PyUnusedLocal
    @staticmethod
    def _create_binary(attr: Attr):
        return LargeBinary

    # noinspection PyUnusedLocal
    @staticmethod
    def _create_bool(attr: Attr):
        return Boolean

    # noinspection PyUnusedLocal
    @staticmethod
    def _create_datetime(attr: Attr):
        return DateTime

    # noinspection PyUnusedLocal
    @staticmethod
    def _create_float(attr: Attr):
        return Float

    # noinspection PyUnusedLocal
    @staticmethod
    def _create_int(attr: Attr):
        return Integer

    # noinspection PyUnusedLocal
    def _create_json(self, attr: Attr):
        if self.dialect == POSTGRES:
            return PostgresJson
        if self.dialect == MYSQL:
            return MysqlJson
        if self.dialect == MSSQL:
            return MssqlJson
        return Text

    def _create_str(self, attr: Attr):
        type_ = attr.schema.python_type
        type_ = get_optional_type(type_) or type_
        if inspect.isclass(type_) and issubclass(type_, Enum):
            return SqlalchemyEnum(type_)
        length = self.get_column_length_from_schema(attr.schema.schema)
        if not length:
            return Text
        return String(length=length)

    # noinspection PyUnusedLocal
    def _create_uuid(self, attr: Attr):
        # AFAIK at the moment, the only dialect with a native UUID type is postgres.
        # So the others will define UUIDs as strings (for readability)
        if self.dialect == POSTGRES:
            return PostgresUuid(as_uuid=True)
        return String(length=36)

    def get_column_length_from_schema(
        self, json_schema: ExternalItemType
    ) -> Optional[int]:
        try:
            return max(self.get_max_lengths_from_schema(json_schema))
        except ValueError:
            return None

    def get_max_lengths_from_schema(self, json_schema) -> Iterator[int]:
        if isinstance(json_schema, dict):
            max_length = json_schema.get("maxLength")
            if max_length:
                yield max_length
            for value in json_schema.values():
                yield from self.get_max_lengths_from_schema(value)
        if isinstance(json_schema, list):
            for value in json_schema:
                yield from self.get_max_lengths_from_schema(value)
