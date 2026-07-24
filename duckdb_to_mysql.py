#!/usr/bin/env python3
"""
Generate TPCH data with DuckDB and load only partsupp into local MySQL.

Uses DuckDB's TPCH extension (dbgen) to generate all 8 TPCH tables at a scale
factor where the full dataset is ~10 GB (SF=10). Only the partsupp table is
exported to MySQL.

Target MySQL (default):
  host=127.0.0.1, port=3306, user=root, password=password, database=tpch

Install dependencies:
  pip install duckdb pymysql

Usage:
  python postgres_to_mysql.py
  python postgres_to_mysql.py --target-gb 10
  python postgres_to_mysql.py --sf 10 --batch-size 5000

Notes:
  - TPCH scale factor maps to total dataset size: SF=1 ~ 1 GB, SF=10 ~ 10 GB (all 8 tables).
  - dbgen always creates all 8 tables; this script loads only partsupp into MySQL.
  - Ensure MySQL is running and you have enough free disk for DuckDB temp files.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any

import duckdb
import pymysql

# TPCH scale factor sizing: SF=1 ~ 1 GB total (all 8 tables), SF=10 ~ 10 GB total.
TOTAL_TPCH_GB_PER_SF = 1.0

TPCH_TABLES = [
    "customer",
    "orders",
    "lineitem",
    "part",
    "partsupp",
    "supplier",
    "nation",
    "region",
]

# Local MySQL from olake/examples/spark-tablurarest-minio-mysql/docker-compose.yml
MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "password",
    "charset": "utf8mb4",
    "autocommit": False,
}

MYSQL_DATABASE = "tpch"
MYSQL_TABLE = "partsupp"
MYSQL_CDC_USER = "cdc_user"

PARTSUPP_COLUMNS = [
    "ps_partkey",
    "ps_suppkey",
    "ps_availqty",
    "ps_supplycost",
    "ps_comment",
]

PARTSUPP_DDL = """
CREATE TABLE IF NOT EXISTS `{table}` (
  `ps_partkey` BIGINT NOT NULL,
  `ps_suppkey` BIGINT NOT NULL,
  `ps_availqty` INT NOT NULL,
  `ps_supplycost` DECIMAL(15,2) NOT NULL,
  `ps_comment` VARCHAR(199) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {secs:.1f}s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {secs:.1f}s"


def sf_for_target_gb(target_gb: float) -> int:
    """Pick SF so the full TPCH dataset (all 8 tables) is ~target_gb."""
    return max(1, round(target_gb / TOTAL_TPCH_GB_PER_SF))


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def connect_duckdb(work_dir: Path, memory_limit: str) -> duckdb.DuckDBPyConnection:
    ensure_dir(work_dir)
    db_path = work_dir / "tpch_partsupp.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA temp_directory='{work_dir / 'tmp'}';")
    con.execute(f"PRAGMA memory_limit='{memory_limit}';")
    return con


def generate_tpch(con: duckdb.DuckDBPyConnection, sf: int) -> int:
    log(
        f"Generating full TPCH dataset at scale factor {sf} "
        f"(~{sf * TOTAL_TPCH_GB_PER_SF:.0f} GB across all 8 tables)..."
    )
    con.execute("INSTALL tpch;")
    con.execute("LOAD tpch;")
    con.execute(f"CALL dbgen(sf={sf})")

    for table in TPCH_TABLES:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log(f"  {table}: {count:,} rows")

    export_count = con.execute(f"SELECT COUNT(*) FROM {MYSQL_TABLE}").fetchone()[0]
    log(f"Only {MYSQL_TABLE} ({export_count:,} rows) will be loaded into MySQL.")

    # Drop the other 7 tables to reclaim disk before the MySQL load.
    for table in TPCH_TABLES:
        if table == MYSQL_TABLE:
            continue
        try:
            con.execute(f"DROP TABLE IF EXISTS {table}")
        except duckdb.Error:
            pass

    return export_count


def prepare_mysql_table(mysql_conn: pymysql.connections.Connection) -> None:
    with mysql_conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}`")
        cur.execute(f"USE `{MYSQL_DATABASE}`")
        cur.execute(f"DROP TABLE IF EXISTS `{MYSQL_TABLE}`")
        cur.execute(PARTSUPP_DDL.format(table=MYSQL_TABLE))
    mysql_conn.commit()


def normalize_value(value: Any) -> Any:
    if isinstance(value, memoryview):
        return bytes(value)
    return value


def copy_partsupp_to_mysql(
    duck_con: duckdb.DuckDBPyConnection,
    mysql_conn: pymysql.connections.Connection,
    batch_size: int,
) -> int:
    columns = PARTSUPP_COLUMNS
    quoted_columns = ", ".join(f"`{name}`" for name in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = (
        f"INSERT INTO `{MYSQL_TABLE}` ({quoted_columns}) VALUES ({placeholders})"
    )

    result = duck_con.execute(f"SELECT {', '.join(columns)} FROM {MYSQL_TABLE}")
    total = 0

    with mysql_conn.cursor() as cur:
        cur.execute(f"USE `{MYSQL_DATABASE}`")
        while True:
            rows = result.fetchmany(batch_size)
            if not rows:
                break
            cur.executemany(
                insert_sql,
                [tuple(normalize_value(v) for v in row) for row in rows],
            )
            mysql_conn.commit()
            total += len(rows)
            if total % (batch_size * 10) == 0 or len(rows) < batch_size:
                log(f"  loaded {total:,} rows into MySQL...")

    return total


def grant_olake_cdc_access(mysql_conn: pymysql.connections.Connection) -> None:
    with mysql_conn.cursor() as cur:
        cur.execute(
            "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT "
            "ON *.* TO %s@'%%'",
            (MYSQL_CDC_USER,),
        )
        cur.execute(
            f"GRANT ALL PRIVILEGES ON `{MYSQL_DATABASE}`.* TO %s@'%%'",
            (MYSQL_CDC_USER,),
        )
        cur.execute("FLUSH PRIVILEGES")
    mysql_conn.commit()


def verify_row_counts(
    duck_con: duckdb.DuckDBPyConnection,
    mysql_conn: pymysql.connections.Connection,
) -> None:
    duck_count = duck_con.execute(f"SELECT COUNT(*) FROM {MYSQL_TABLE}").fetchone()[0]
    with mysql_conn.cursor() as cur:
        cur.execute(f"USE `{MYSQL_DATABASE}`")
        cur.execute(f"SELECT COUNT(*) FROM `{MYSQL_TABLE}`")
        mysql_count = cur.fetchone()[0]

    log(f"DuckDB {MYSQL_TABLE} rows: {duck_count:,}")
    log(f"MySQL {MYSQL_TABLE} rows:  {mysql_count:,}")
    if duck_count != mysql_count:
        raise RuntimeError("Row count mismatch after load")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate full TPCH dataset, load only partsupp into MySQL"
    )
    parser.add_argument(
        "--target-gb",
        type=float,
        default=10.0,
        help="Approximate total TPCH dataset size in GB, all 8 tables (default: 10 -> SF=10)",
    )
    parser.add_argument(
        "--sf",
        type=int,
        default=None,
        help="TPCH scale factor override (if set, ignores --target-gb)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="MySQL insert batch size (default: 5000)",
    )
    parser.add_argument(
        "--work-dir",
        default="./tpch_workdir",
        help="Directory for DuckDB files and temp spill (default: ./tpch_workdir)",
    )
    parser.add_argument(
        "--duckdb-memory-limit",
        default="8GB",
        help="DuckDB memory_limit pragma (default: 8GB)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sf = args.sf if args.sf is not None else sf_for_target_gb(args.target_gb)
    work_dir = Path(args.work_dir).resolve()

    log("TPCH generator -> MySQL partsupp loader")
    log(
        f"total_dataset_target_gb={args.target_gb} scale_factor={sf} "
        f"(generates all 8 tables, loads only partsupp)"
    )
    log(f"work_dir={work_dir}")
    log(f"mysql={MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_DATABASE}.{MYSQL_TABLE}")

    duck_con = connect_duckdb(work_dir, args.duckdb_memory_limit)
    mysql_conn = pymysql.connect(**MYSQL_CONFIG)

    try:
        generate_tpch(duck_con, sf)

        log("Preparing MySQL database and table...")
        prepare_mysql_table(mysql_conn)

        log(f"Loading {MYSQL_TABLE} into MySQL...")
        load_start = time.perf_counter()
        loaded = copy_partsupp_to_mysql(duck_con, mysql_conn, args.batch_size)
        load_elapsed = time.perf_counter() - load_start
        log(f"Loaded {loaded:,} rows into MySQL in {format_duration(load_elapsed)}.")

        log("Verifying row counts...")
        verify_row_counts(duck_con, mysql_conn)

        log(f"Granting OLake CDC user `{MYSQL_CDC_USER}` access...")
        grant_olake_cdc_access(mysql_conn)

        log("Done.")
        return 0
    finally:
        duck_con.close()
        mysql_conn.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        log(f"ERROR: {exc}")
        raise SystemExit(1) from exc
