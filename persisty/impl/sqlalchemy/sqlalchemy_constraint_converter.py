from dataclasses import dataclass
from typing import Dict, Iterator

from sqlalchemy import ForeignKeyConstraint

from persisty.key_config.field_key_config import FieldKeyConfig
from persisty.link.belongs_to import BelongsTo
from persisty.link.on_delete import OnDelete
from persisty.storage.storage_meta import StorageMeta


@dataclass
class SqlalchemyConstraintConverter:
    schema: Dict[str, StorageMeta]

    def get_foreign_key_constraints(
        self, storage_meta: StorageMeta
    ) -> Iterator[ForeignKeyConstraint]:
        for link in storage_meta.links:
            if isinstance(link, BelongsTo):
                linked_storage = self.schema.get(link.storage_name)
                if linked_storage and isinstance(
                    linked_storage.key_config, FieldKeyConfig
                ):
                    kwargs = {}
                    if link.on_delete == OnDelete.CASCADE:
                        kwargs["ondelete"] = "CASCADE"
                    elif link.on_delete == OnDelete.NULLIFY:
                        kwargs["ondelete"] = "SET NULL"
                    constraint = ForeignKeyConstraint(
                        (link.id_field_name,),
                        (
                            f"{link.storage_name}.{linked_storage.key_config.field_name}",
                        ),
                        **kwargs,
                    )
                    yield constraint
