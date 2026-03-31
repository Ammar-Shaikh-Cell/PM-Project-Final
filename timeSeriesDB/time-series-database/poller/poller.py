"""
MSSQL → TimescaleDB poller: introspects Tab_Actual, creates hypertable,
backfills then incrementally polls with watermark-based sync.
"""

import os
import signal
import sys
import time
import logging
from datetime import datetime, timezone
from typing import List, Tuple, Optional, Any

import pymssql
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ------------ Logging configuration ------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("poller")

# ------------ Global configuration ------------

MSSQL_HOST = os.getenv("MSSQL_HOST")
MSSQL_PORT = int(os.getenv("MSSQL_PORT", "1433"))
MSSQL_USER = os.getenv("MSSQL_USER")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_TABLE = os.getenv("MSSQL_TABLE", "Tab_Actual")

TSDB_HOST = os.getenv("TSDB_HOST", "timescaledb")
TSDB_PORT = int(os.getenv("TSDB_PORT", "5432"))
TSDB_USER = os.getenv("TSDB_USER")
TSDB_PASSWORD = os.getenv("TSDB_PASSWORD")
TSDB_DATABASE = os.getenv("TSDB_DATABASE")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2

# Set at startup after schema introspection
TIME_COLUMN_NAME: Optional[str] = None
PK_COLUMN_NAMES: List[str] = []
CONFLICT_COLS: List[str] = []  # time + PK columns for unique index and ON CONFLICT
COLUMN_DEFS: List[Tuple[str, str]] = []

# Optional column allowlist for TimescaleDB target.
# When non-empty, only these MSSQL columns will be created/inserted in Tab_Actual.
ALLOWED_TSDB_COLUMNS: List[str] = [
    "Idx",
    "TrendDate",
    "Val_1",
    "Val_2",
    "Val_3",
    "Val_4",
    "Val_5",
    "Val_6",
    "Val_7",
    "Val_8",
    "Val_9",
    "Val_10",
    "Val_11",
    "Val_12",
    "Val_14",
    "Val_15",
    "Val_19",
    "Val_20",
    "Val_21",
    "Val_22",
    "Val_23",
    "Val_27",
    "Val_28",
    "Val_29",
    "Val_30",
    "Val_31",
    "Val_32",
    "Val_33",
    "Val_34",
    "Val_35",
    "Val_36",
    "Val_37",
    "Val_38",
    "Val_39",
    "Val_40",
    "Val_41",
    "Val_42",
    "Val_43",
    "Val_44",
    "Val_45",
    "Val_46",
    "Val_47",
    "Val_48",
]

SHOULD_STOP = False


# ------------ Signal handling ------------

def handle_signal(signum, frame):
    global SHOULD_STOP
    logger.info("Received shutdown signal (%s). Stopping gracefully...", signum)
    SHOULD_STOP = True


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


# ------------ Retry helper ------------

def with_retries(func):
    def wrapper(*args, **kwargs):
        attempt = 0
        backoff = INITIAL_BACKOFF_SECONDS
        while True:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                attempt += 1
                if attempt > MAX_RETRIES:
                    logger.exception("Operation failed after %s retries.", MAX_RETRIES)
                    raise
                logger.warning(
                    "Operation failed (attempt %s/%s): %s. Retrying in %s seconds...",
                    attempt,
                    MAX_RETRIES,
                    e,
                    backoff,
                )
                time.sleep(backoff)
                backoff *= 2
    return wrapper


# ------------ Connections ------------

@with_retries
def get_mssql_connection() -> pymssql.Connection:
    logger.debug("Connecting to MSSQL...")
    conn = pymssql.connect(
        server=MSSQL_HOST,
        port=MSSQL_PORT,
        user=MSSQL_USER,
        password=MSSQL_PASSWORD,
        database=MSSQL_DATABASE,
        login_timeout=5,
        timeout=5,
    )
    return conn


@with_retries
def get_pg_connection() -> psycopg2.extensions.connection:
    logger.debug("Connecting to TimescaleDB...")
    conn = psycopg2.connect(
        host=TSDB_HOST,
        port=TSDB_PORT,
        user=TSDB_USER,
        password=TSDB_PASSWORD,
        dbname=TSDB_DATABASE,
    )
    conn.autocommit = False
    return conn


# ------------ Type mapping & schema introspection ------------

def map_mssql_to_pg_type(mssql_type: str, char_max_len: Optional[int]) -> str:
    t = mssql_type.lower()
    if t in ("datetime", "datetime2", "smalldatetime", "datetimeoffset", "date", "time"):
        return "TIMESTAMPTZ"
    if t in ("float", "real"):
        return "DOUBLE PRECISION"
    if t in ("int", "integer"):
        return "INTEGER"
    if t in ("bigint",):
        return "BIGINT"
    if t in ("smallint",):
        return "SMALLINT"
    if t in ("tinyint",):
        return "SMALLINT"
    if t in ("bit",):
        return "BOOLEAN"
    if t in ("decimal", "numeric", "money", "smallmoney"):
        return "NUMERIC"
    if t in ("char", "nchar", "varchar", "nvarchar", "text", "ntext"):
        return "TEXT"
    if t in ("binary", "varbinary", "image"):
        return "BYTEA"
    return "TEXT"


@with_retries
def get_mssql_schema() -> List[Tuple[str, str]]:
    """
    Returns list of (column_name, pg_data_type) tuples for MSSQL_TABLE.
    Sets global TIME_COLUMN_NAME and COLUMN_DEFS.
    """
    global TIME_COLUMN_NAME, COLUMN_DEFS

    conn = get_mssql_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (MSSQL_TABLE,),
        )
        rows = cursor.fetchall()
        if not rows:
            raise RuntimeError(f"No columns found for table {MSSQL_TABLE} in MSSQL.")

        column_defs: List[Tuple[str, str]] = []
        datetime_candidates: List[str] = []

        for col_name, data_type, char_len in rows:
            pg_type = map_mssql_to_pg_type(data_type, char_len)
            column_defs.append((col_name, pg_type))
            if pg_type == "TIMESTAMPTZ":
                datetime_candidates.append(col_name)

        if not datetime_candidates:
            raise RuntimeError(
                f"No datetime-like columns found on {MSSQL_TABLE}; cannot determine time column."
            )

        time_col = None
        for c in datetime_candidates:
            if c.lower() == "time":
                time_col = c
                break
        if time_col is None:
            time_col = datetime_candidates[0]

        TIME_COLUMN_NAME = time_col

        # If an allowlist is defined, only keep those columns for the TimescaleDB target.
        if ALLOWED_TSDB_COLUMNS:
            filtered_defs: List[Tuple[str, str]] = []
            for name, pg_type in column_defs:
                if name in ALLOWED_TSDB_COLUMNS:
                    filtered_defs.append((name, pg_type))
                else:
                    logger.info("Skipping column %s (not in TSDB allowlist)", name)
            COLUMN_DEFS = filtered_defs
        else:
            COLUMN_DEFS = column_defs

        logger.info("Detected MSSQL schema for %s:", MSSQL_TABLE)
        for name, pg_type in COLUMN_DEFS:
            logger.info("  %s => %s", name, pg_type)
        logger.info("Using '%s' as time partition column.", TIME_COLUMN_NAME)

        return COLUMN_DEFS
    finally:
        conn.close()


@with_retries
def get_mssql_primary_keys() -> List[str]:
    """Returns list of primary key column names in MSSQL_TABLE."""
    conn = get_mssql_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            WHERE tc.TABLE_NAME = %s
              AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY kcu.ORDINAL_POSITION
            """,
            (MSSQL_TABLE,),
        )
        rows = cursor.fetchall()
        pk_cols = [r[0] for r in rows]
        logger.info("Detected MSSQL primary key columns: %s", pk_cols if pk_cols else "None")
        return pk_cols
    finally:
        conn.close()


# ------------ TimescaleDB schema management ------------

@with_retries
def ensure_hypertable_exists(schema: List[Tuple[str, str]]) -> None:
    """
    Creates the target table and hypertable in TimescaleDB if they do not exist.
    Configures unique index and compression policy.
    """
    global PK_COLUMN_NAMES, CONFLICT_COLS

    PK_COLUMN_NAMES = get_mssql_primary_keys()
    CONFLICT_COLS = [TIME_COLUMN_NAME] + PK_COLUMN_NAMES if PK_COLUMN_NAMES else [TIME_COLUMN_NAME]
    # Ensure Idx is in CONFLICT_COLS when present (for ON CONFLICT); MSSQL may not expose it as PK
    schema_col_names = [c[0] for c in schema]
    if "Idx" in schema_col_names and "Idx" not in CONFLICT_COLS:
        CONFLICT_COLS = CONFLICT_COLS + ["Idx"]

    conn = get_pg_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS _poller_state (
                id              INTEGER PRIMARY KEY,
                last_watermark  TIMESTAMPTZ
            )
            """
        )
        cur.execute(
            """
            INSERT INTO _poller_state (id, last_watermark)
            VALUES (1, NULL)
            ON CONFLICT (id) DO NOTHING
            """
        )

        columns_sql_parts = []
        for col_name, pg_type in schema:
            columns_sql_parts.append(
                sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(pg_type))
            )

        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({});").format(
            sql.Identifier(MSSQL_TABLE),
            sql.SQL(", ").join(columns_sql_parts),
        )
        logger.info("Ensuring target table %s exists in TimescaleDB...", MSSQL_TABLE)
        cur.execute(create_table_query)

        logger.info("Ensuring hypertable exists for %s...", MSSQL_TABLE)
        # Pass table as regclass with quoted identifier; pass raw column name for time column
        quoted_table = f'"{MSSQL_TABLE}"'
        cur.execute(
            """
            SELECT create_hypertable(%s::regclass, %s, if_not_exists => TRUE);
            """,
            (quoted_table, TIME_COLUMN_NAME),
        )

        # set_chunk_time_interval expects a table regclass, so pass the same quoted_table
        cur.execute(
            "SELECT set_chunk_time_interval(%s::regclass, INTERVAL '1 day');",
            (quoted_table,),
        )

        unique_index_name = f"{MSSQL_TABLE.lower()}_uniq_time_pk"
        idx_sql = sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});").format(
            sql.Identifier(unique_index_name),
            sql.Identifier(MSSQL_TABLE),
            sql.SQL(", ").join(sql.Identifier(c) for c in CONFLICT_COLS),
        )
        cur.execute(idx_sql)

        # Enable compression; let TimescaleDB use default segmentby (optional)
        cur.execute(
            sql.SQL(
                """
                ALTER TABLE {} SET (
                    timescaledb.compress
                );
                """
            ).format(sql.Identifier(MSSQL_TABLE)),
        )

        try:
            cur.execute(
                "SELECT add_compression_policy(%s::regclass, INTERVAL '7 days');",
                (quoted_table,),
            )
        except Exception as e:
            logger.info("Compression policy might already exist: %s", e)

        conn.commit()
        logger.info("Hypertable and policies ensured.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ------------ Watermark management ------------

@with_retries
def get_watermark() -> Optional[datetime]:
    """Reads last synced timestamp from _poller_state."""
    conn = get_pg_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT last_watermark FROM _poller_state WHERE id = 1;")
        row = cur.fetchone()
        if row and row[0] is not None:
            wm = row[0]
            if wm.tzinfo is None:
                wm = wm.replace(tzinfo=timezone.utc)
            return wm
        return None
    finally:
        conn.close()


@with_retries
def save_watermark(ts: datetime) -> None:
    """Persists watermark after successful batch insert."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    conn = get_pg_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO _poller_state (id, last_watermark)
            VALUES (1, %s)
            ON CONFLICT (id) DO UPDATE SET last_watermark = EXCLUDED.last_watermark
            """,
            (ts,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ------------ Fetch from MSSQL ------------

@with_retries
def fetch_batch(since_ts: Optional[datetime], limit: int) -> List[Tuple[Any, ...]]:
    """
    Queries MSSQL for new rows since watermark.
    Uses >= watermark; duplicates handled by ON CONFLICT DO NOTHING in target.
    """
    if TIME_COLUMN_NAME is None:
        raise RuntimeError("TIME_COLUMN_NAME not set.")

    conn = get_mssql_connection()
    try:
        cursor = conn.cursor()
        column_names = [c[0] for c in COLUMN_DEFS]
        col_list_sql = ", ".join(f"[{c}]" for c in column_names)

        if since_ts is None:
            # SQL Server 2000 syntax: TOP <n> (no parentheses)
            query = f"""
                SELECT TOP {limit} {col_list_sql}
                FROM [{MSSQL_TABLE}]
                ORDER BY [{TIME_COLUMN_NAME}] ASC
            """
            cursor.execute(query)
        else:
            # Use strict '>' to avoid getting stuck on rows with the same timestamp as watermark
            query = f"""
                SELECT TOP {limit} {col_list_sql}
                FROM [{MSSQL_TABLE}]
                WHERE [{TIME_COLUMN_NAME}] > %s
                ORDER BY [{TIME_COLUMN_NAME}] ASC
            """
            cursor.execute(query, (since_ts,))
        rows = cursor.fetchall()
        return [tuple(r) for r in rows]
    finally:
        conn.close()


# ------------ Insert into TimescaleDB ------------

def _normalize_row_for_pg(row: Tuple[Any, ...], columns: List[str]) -> Tuple[Any, ...]:
    """Convert MSSQL row values for PostgreSQL (e.g. naive datetime → timestamptz)."""
    out = []
    for i, val in enumerate(row):
        if val is None:
            out.append(None)
        elif isinstance(val, datetime) and val.tzinfo is None:
            out.append(val.replace(tzinfo=timezone.utc))
        elif isinstance(val, bool):
            out.append(val)
        else:
            out.append(val)
    return tuple(out)


@with_retries
def insert_batch(rows: List[Tuple[Any, ...]], columns: List[str]) -> None:
    """
    Bulk inserts into TimescaleDB using execute_values with ON CONFLICT DO NOTHING.
    """
    if not rows:
        return

    conn = get_pg_connection()
    try:
        cur = conn.cursor()
        normalized = [_normalize_row_for_pg(r, columns) for r in rows]

        col_identifiers = [sql.Identifier(c) for c in columns]
        conflict_cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in CONFLICT_COLS)
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO NOTHING").format(
            sql.Identifier(MSSQL_TABLE),
            sql.SQL(", ").join(col_identifiers),
            conflict_cols_sql,
        )
        execute_values(
            cur,
            insert_query.as_string(cur),
            normalized,
            page_size=1000,
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("Failed to insert batch into TimescaleDB: %s", e)
        logger.error("Offending batch sample (first 5 rows): %s", rows[:5])
        raise
    finally:
        conn.close()


# ------------ Backfill & Poller loops ------------

def run_backfill() -> None:
    """Performs a full backfill of all historical data."""
    logger.info("Starting backfill process...")
    while not SHOULD_STOP:
        try:
            current_wm = get_watermark()
            rows = fetch_batch(current_wm, BATCH_SIZE)
        except Exception as e:
            logger.error("Backfill fetch error: %s", e)
            if SHOULD_STOP:
                break
            time.sleep(INITIAL_BACKOFF_SECONDS)
            continue

        if not rows:
            logger.info("Backfill complete: no more historical rows to copy.")
            break

        columns = [c[0] for c in COLUMN_DEFS]
        try:
            insert_batch(rows, columns)
        except Exception as e:
            logger.error("Backfill insert error (skipping batch): %s", e)
            continue

        time_idx = columns.index(TIME_COLUMN_NAME)
        max_ts = max((row[time_idx] for row in rows if row[time_idx] is not None), default=None)
        if isinstance(max_ts, datetime):
            save_watermark(max_ts)
            logger.info(
                "Backfill batch: fetched=%d, inserted=%d, watermark=%s",
                len(rows),
                len(rows),
                max_ts.isoformat(),
            )
        else:
            logger.warning("Time column is not datetime-like in backfill batch.")

    logger.info("Backfill finished or stopped.")


def run_poller() -> None:
    """Infinite loop: fetch → insert → save watermark → sleep."""
    logger.info(
        "Starting incremental poller, interval=%ss, batch_size=%d",
        POLL_INTERVAL_SECONDS,
        BATCH_SIZE,
    )
    while not SHOULD_STOP:
        cycle_start = time.time()
        try:
            current_wm = get_watermark()
            rows = fetch_batch(current_wm, BATCH_SIZE)

            if rows:
                columns = [c[0] for c in COLUMN_DEFS]
                try:
                    insert_batch(rows, columns)
                except Exception as e:
                    logger.error("Poll cycle insert error: %s", e)
                else:
                    time_idx = columns.index(TIME_COLUMN_NAME)
                    max_ts = max(
                        (row[time_idx] for row in rows if row[time_idx] is not None),
                        default=None,
                    )
                    if isinstance(max_ts, datetime):
                        save_watermark(max_ts)
                        logger.info(
                            "Poll cycle: fetched=%d, inserted=%d, watermark=%s",
                            len(rows),
                            len(rows),
                            max_ts.isoformat(),
                        )
                    else:
                        logger.warning("Time column is not datetime-like in poll batch.")
            else:
                logger.info("Poll cycle: no new rows; current watermark=%s", current_wm)

        except Exception as e:
            logger.error("Error during poll cycle: %s", e)

        elapsed = time.time() - cycle_start
        sleep_time = max(0, POLL_INTERVAL_SECONDS - elapsed)
        if sleep_time > 0 and not SHOULD_STOP:
            time.sleep(sleep_time)

    logger.info("Poller loop stopped gracefully.")


# ------------ Main entrypoint ------------

def main() -> None:
    logger.info("Poller starting up...")

    required_envs = [
        ("MSSQL_HOST", MSSQL_HOST),
        ("MSSQL_USER", MSSQL_USER),
        ("MSSQL_PASSWORD", MSSQL_PASSWORD),
        ("MSSQL_DATABASE", MSSQL_DATABASE),
        ("TSDB_HOST", TSDB_HOST),
        ("TSDB_USER", TSDB_USER),
        ("TSDB_PASSWORD", TSDB_PASSWORD),
        ("TSDB_DATABASE", TSDB_DATABASE),
    ]
    missing = [name for name, value in required_envs if not value]
    if missing:
        logger.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)

    schema = get_mssql_schema()
    ensure_hypertable_exists(schema)

    run_backfill()
    if not SHOULD_STOP:
        run_poller()


if __name__ == "__main__":
    main()
