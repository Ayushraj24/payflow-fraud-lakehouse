from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from typing import Any

import duckdb

try:
    from .config import DB_PATH, ensure_directories
except ImportError:  # pragma: no cover - supports running scripts directly
    from config import DB_PATH, ensure_directories


def connect(db_path: str | Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    ensure_directories()
    return duckdb.connect(str(db_path))


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def remove_path(path: str | Path) -> None:
    target = Path(path)
    if target.is_dir():
        shutil.rmtree(target)
    elif target.exists():
        target.unlink()


def init_metadata(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR PRIMARY KEY,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            status VARCHAR NOT NULL,
            raw_file VARCHAR,
            sample_rows BIGINT,
            bronze_rows BIGINT,
            quarantine_rows BIGINT,
            silver_rows BIGINT,
            gold_daily_rows BIGINT,
            notes VARCHAR
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS quality_results (
            quality_run_id VARCHAR,
            checked_at TIMESTAMP NOT NULL,
            check_name VARCHAR NOT NULL,
            failed_rows BIGINT NOT NULL,
            total_rows BIGINT NOT NULL,
            status VARCHAR NOT NULL
        )
        """
    )

    quality_columns = {
        row[1] for row in con.execute("PRAGMA table_info('quality_results')").fetchall()
    }
    if "quality_run_id" not in quality_columns:
        con.execute("ALTER TABLE quality_results ADD COLUMN quality_run_id VARCHAR")


def start_pipeline_run(
    con: duckdb.DuckDBPyConnection,
    raw_file: str | Path,
    sample_rows: int | None,
) -> str:
    init_metadata(con)
    run_id = str(uuid.uuid4())
    con.execute(
        """
        INSERT INTO pipeline_runs (
            run_id, started_at, status, raw_file, sample_rows, notes
        )
        VALUES (?, current_timestamp, 'RUNNING', ?, ?, NULL)
        """,
        [run_id, str(raw_file), sample_rows],
    )
    return run_id


def finish_pipeline_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    status: str,
    metrics: dict[str, Any] | None = None,
    notes: str | None = None,
) -> None:
    metrics = metrics or {}
    con.execute(
        """
        UPDATE pipeline_runs
        SET
            finished_at = current_timestamp,
            status = ?,
            bronze_rows = ?,
            quarantine_rows = ?,
            silver_rows = ?,
            gold_daily_rows = ?,
            notes = ?
        WHERE run_id = ?
        """,
        [
            status,
            metrics.get("bronze_rows"),
            metrics.get("quarantine_rows"),
            metrics.get("silver_rows"),
            metrics.get("gold_daily_rows"),
            notes,
            run_id,
        ],
    )


def parquet_glob(path: str | Path) -> str:
    source = Path(path)
    if source.is_dir():
        return str(source / "**" / "*.parquet")
    return str(source)


def has_parquet(path: str | Path) -> bool:
    source = Path(path)
    if source.is_file():
        return True
    if source.is_dir():
        return any(source.rglob("*.parquet"))
    return False


def create_or_replace_parquet_view(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    path: str | Path,
    *,
    hive_partitioning: bool = False,
) -> bool:
    if not has_parquet(path):
        return False

    hive = "true" if hive_partitioning else "false"
    con.execute(
        f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT *
        FROM read_parquet({sql_string(parquet_glob(path))}, hive_partitioning = {hive})
        """
    )
    return True


def count_relation(con: duckdb.DuckDBPyConnection, relation_name: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {relation_name}").fetchone()[0])
