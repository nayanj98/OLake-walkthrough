#!/usr/bin/env python3
"""
Continuously update tpch.partsupp in local MySQL for CDC testing.

Fastest mode: always update the first N rows in storage order using LIMIT
(no WHERE, no ORDER BY RAND(), no index needed). Same rows each run, but
each update still emits CDC/binlog events.

MySQL target (spark-tablurarest-minio-mysql docker-compose):
  host=127.0.0.1, port=3306, user=root, password=password, database=tpch

Usage:
  python3 continuous_update_partsupp.py

Tune ROWS_PER_RUN below:
  - 50      -> fastest (~milliseconds), 50 CDC events/min
  - 5,000   -> very fast (~1s), 5k CDC events/min
  - 50,000  -> fast (~few seconds), 50k CDC events/min
"""

import time

import pymysql

# Local MySQL from olake/examples/spark-tablurarest-minio-mysql/docker-compose.yml
HOST = "127.0.0.1"
PORT = 3306
USER = "root"
PASSWORD = "password"
DATABASE = "tpch"
TABLE = "partsupp"

INTERVAL_SECONDS = 60
ROWS_PER_RUN = 500_000
SUPPLY_COST_INCREMENT = 10

UPDATE_SQL = f"""
    UPDATE `{TABLE}`
    SET ps_supplycost = ps_supplycost + %s
    LIMIT {ROWS_PER_RUN}
"""


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(seconds, 60)
    return f"{int(minutes)}m {secs:.1f}s"


def main() -> None:
    print(f"Connecting to {HOST}:{PORT}/{DATABASE} ...")

    conn = None
    cursor = None

    try:
        conn = pymysql.connect(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE,
            charset="utf8mb4",
            autocommit=False,
        )
        cursor = conn.cursor()
        print("Connected successfully.")

        cursor.execute(f"SELECT COUNT(*) FROM `{TABLE}`")
        row_count = cursor.fetchone()[0]
        print(f"Table          : {DATABASE}.{TABLE}")
        print(f"Total rows     : {row_count:,}")
        print(f"Schedule       : every {INTERVAL_SECONDS}s (press Ctrl+C to stop)")
        print(
            f"Rows per run   : first {ROWS_PER_RUN:,} rows (LIMIT, ps_supplycost + "
            f"{SUPPLY_COST_INCREMENT})"
        )

        if row_count == 0:
            raise RuntimeError(f"{DATABASE}.{TABLE} is empty. Run postgres_to_mysql.py first.")

        run_idx = 0
        next_run_monotonic = time.monotonic()

        while True:
            if run_idx > 0:
                sleep_seconds = next_run_monotonic - time.monotonic()
                if sleep_seconds > 0:
                    print(f"  Sleeping {round(sleep_seconds, 1)}s until next run ...")
                    time.sleep(sleep_seconds)
            next_run_monotonic += INTERVAL_SECONDS

            run_idx += 1
            print(
                f"\n[run {run_idx}] Updating first {ROWS_PER_RUN:,} rows in "
                f"{DATABASE}.{TABLE} (+{SUPPLY_COST_INCREMENT} to ps_supplycost) ..."
            )

            start = time.perf_counter()
            cursor.execute(UPDATE_SQL, (SUPPLY_COST_INCREMENT,))
            rows_affected = cursor.rowcount
            conn.commit()
            elapsed = time.perf_counter() - start

            print(
                f"[run {run_idx}] Done. Rows updated: {rows_affected:,} | "
                f"Time taken: {format_duration(elapsed)}"
            )

    except KeyboardInterrupt:
        print("\nStopped by user.")

    except Exception as exc:
        print(f"ERROR: {exc}")
        if conn:
            conn.rollback()
            print("Transaction rolled back.")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    main()
