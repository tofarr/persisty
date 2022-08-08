import os
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from persisty.util import get_logger

logger = get_logger(__name__)


@lru_cache()
def get_default_engine() -> Engine:
    sql_urn = os.environ.get("SQL_URN") or "sqlite+pysqlite:///:memory:"
    sql_echo = (os.environ.get("SQL_ECHO") or "").lower() not in ("false", "0")
    if not os.environ.get("PERSISTY_SQL_URN"):
        logger.warning(f"PERSISTY_SQL_URN NOT SET: USING {sql_urn}")
    engine = create_engine(sql_urn, echo=sql_echo, future=True)
    return engine
