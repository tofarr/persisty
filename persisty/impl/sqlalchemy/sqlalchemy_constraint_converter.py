from dataclasses import dataclass
from typing import Dict, Iterator

from sqlalchemy import ForeignKeyConstraint

from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.link.belongs_to import BelongsTo
from persisty.link.on_delete import OnDelete
from persisty.store_meta import StoreMeta


@dataclass
class SqlalchemyConstraintConverter:
    schema: Dict[str, StoreMeta]

    def get_foreign_key_constraints(
        self, store_meta: StoreMeta
    ) -> Iterator[ForeignKeyConstraint]:
        for link in store_meta.links:
            if isinstance(link, BelongsTo):
                linked_store = self.schema.get(link.linked_store_name)
                if linked_store and isinstance(linked_store.key_config, AttrKeyConfig):
                    kwargs = {}
                    if link.on_delete == OnDelete.CASCADE:
                        kwargs["ondelete"] = "CASCADE"
                    elif link.on_delete == OnDelete.NULLIFY:
                        kwargs["ondelete"] = "SET NULL"
                    constraint = ForeignKeyConstraint(
                        (link.key_attr_name,),
                        (
                            f"{link.linked_store_name}.{linked_store.key_config.attr_name}",
                        ),
                        **kwargs,
                    )
                    yield constraint
