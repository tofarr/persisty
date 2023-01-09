import logging
import os
from typing import Optional

from sqlalchemy import create_engine

from persisty.impl.sqlalchemy.sqlalchemy_context import SqlalchemyContext
from persisty.impl.sqlalchemy.sqlalchemy_context_factory_abc import (
    SqlalchemyContextFactoryABC,
)

LOGGER = logging.getLogger(__name__)


class SqlalchemyContextFactory(SqlalchemyContextFactoryABC):
    def create(self) -> Optional[SqlalchemyContext]:
        sql_urn = os.environ.get("PERSISTY_SQL_URN")
        # If we have not defined an sql urn, then we are in developer mode, where tables are created on the fly
        developer_mode = not sql_urn or os.environ.get("PERSISTY_DEVELOPER_MODE") == "1"
        if not sql_urn:
            sql_urn = "sqlite+pysqlite:///:memory:"
        sql_echo = (os.environ.get("SQL_ECHO") or "").lower() not in ("false", "0")
        if not os.environ.get("PERSISTY_SQL_URN"):
            LOGGER.warning(f"PERSISTY_SQL_URN NOT SET: USING {sql_urn}")
        engine = create_engine(sql_urn, echo=sql_echo, future=True)
        return SqlalchemyContext(engine, developer_mode)
