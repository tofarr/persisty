"""
Use the config mechanism for hooks
"""
import logging
import os
from functools import lru_cache


SQL_URN = os.environ.get('SQL_URN') or "sqlite+pysqlite:///:memory:"
SQL_ECHO = (os.environ.get("SQL_ECHO") or '').lower() not in ('false', '0')
logger = logging.getLogger(__name__)
if not os.environ.get('USERY_SQL_URN'):
    logger.warning(f'USERY_SQL_URN NOT SET: USING {SQL_URN}')


@lru_cache()
def get_engine():
    from sqlalchemy import create_engine
    engine = create_engine(SQL_URN, echo=SQL_ECHO, future=True)
    return engine
