# core/database.py
# PostgreSQL connection pool — drop-in replacement for bare psycopg2.connect().
# All existing bot code (conn.cursor(), conn.commit(), conn.close()) works
# unchanged. conn.close() returns the connection to the pool instead of
# destroying it.

from __future__ import annotations

import logging
import threading
import warnings

import psycopg2
import psycopg2.extensions
from psycopg2 import pool as pg_pool
from contextlib import contextmanager
from typing import Iterator

warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

from core.config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT

logger = logging.getLogger(__name__)

_POOL: pg_pool.ThreadedConnectionPool | None = None
_POOL_LOCK = threading.Lock()
_POOL_MIN = 2
_POOL_MAX = 20  # Increased from 8 → supports more concurrent bots


def _get_pool() -> pg_pool.ThreadedConnectionPool:
    global _POOL
    if _POOL is not None and not _POOL.closed:
        return _POOL
    with _POOL_LOCK:
        if _POOL is not None and not _POOL.closed:
            return _POOL
        logger.info(
            f"Creating DB connection pool "
            f"(min={_POOL_MIN}, max={_POOL_MAX}) → {DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        _POOL = pg_pool.ThreadedConnectionPool(
            minconn=_POOL_MIN,
            maxconn=_POOL_MAX,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            options="-c lock_timeout=30000",
        )
    return _POOL


class PooledConnection:
    """
    Wraps a real psycopg2 connection, returning it to the pool on close()
    instead of destroying it. Forwards all attributes/methods transparently.
    """

    def __init__(self, conn: psycopg2.extensions.connection) -> None:
        object.__setattr__(self, "_conn", conn)

    def __getattr__(self, name: str):
        return getattr(object.__getattribute__(self, "_conn"), name)

    def __setattr__(self, name: str, value) -> None:
        if name == "_conn":
            object.__setattr__(self, name, value)
        else:
            setattr(object.__getattribute__(self, "_conn"), name, value)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self) -> None:
        conn = object.__getattribute__(self, "_conn")
        try:
            if not conn.closed:
                conn.rollback()
            _get_pool().putconn(conn)
        except Exception as e:
            logger.warning(f"Error returning connection to pool: {e}")
            try:
                conn.close()
            except Exception:
                pass

    def cursor(self, *args, **kwargs):
        return object.__getattribute__(self, "_conn").cursor(*args, **kwargs)

    def commit(self) -> None:
        object.__getattribute__(self, "_conn").commit()

    def rollback(self) -> None:
        object.__getattribute__(self, "_conn").rollback()


def get_db_connection() -> PooledConnection:
    """Returns a pooled connection. Drop-in for psycopg2.connect()."""
    try:
        raw = _get_pool().getconn()
        return PooledConnection(raw)
    except pg_pool.PoolError as e:
        logger.error(f"Pool exhausted or error: {e}")
        raise
    except Exception as e:
        logger.error(f"Critical DB connection error: {e}")
        raise


def release_db_connection(conn: PooledConnection) -> None:
    """Explicit pool return — not needed when using close() or context manager."""
    conn.close()


@contextmanager
def db_connection() -> Iterator[PooledConnection]:
    """
    Recommended context manager for new code:

        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
            conn.commit()
    """
    conn = get_db_connection()
    try:
        yield conn
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def close_pool() -> None:
    """Closes all pool connections — call on clean shutdown."""
    global _POOL
    if _POOL is not None and not _POOL.closed:
        _POOL.closeall()
        logger.info("DB connection pool closed.")
