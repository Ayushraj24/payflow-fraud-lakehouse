from __future__ import annotations

import argparse
from pathlib import Path

try:
    from .config import resolve_raw_file
    from .db import connect, finish_pipeline_run, start_pipeline_run
    from .ingest import run_ingest
    from .quality import run_quality
    from .transform import run_transform
except ImportError:  # pragma: no cover - supports running scripts directly
    from config import resolve_raw_file
    from db import connect, finish_pipeline_run, start_pipeline_run
    from ingest import run_ingest
    from quality import run_quality
    from transform import run_transform


def run_pipeline(
    raw_file: str | Path | None = None,
    sample_rows: int | None = None,
    db_path: str | Path | None = None,
) -> dict[str, int | str]:
    source_file = resolve_raw_file(raw_file)
    con = connect(db_path) if db_path else connect()
    run_id = start_pipeline_run(con, source_file, sample_rows)
    con.close()

    metrics: dict[str, int | str] = {"run_id": run_id, "raw_file": str(source_file)}
    try:
        metrics.update(run_ingest(source_file, sample_rows, db_path))
        metrics.update(run_quality(db_path))
        metrics.update(run_transform(db_path))

        con = connect(db_path) if db_path else connect()
        finish_pipeline_run(con, run_id, "SUCCESS", metrics)
        con.close()
        return metrics
    except Exception as exc:
        con = connect(db_path) if db_path else connect()
        finish_pipeline_run(con, run_id, "FAILED", metrics, notes=str(exc))
        con.close()
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full PayFlow lakehouse pipeline.")
    parser.add_argument("--raw-file", default=None, help="Path to the PaySim CSV.")
    parser.add_argument("--sample-rows", type=int, default=None, help="Limit rows for a smoke run.")
    parser.add_argument("--db-path", default=None, help="DuckDB metadata database path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    metrics = run_pipeline(args.raw_file, args.sample_rows, args.db_path)
    print("Pipeline run complete")
    print(f"Run ID: {metrics['run_id']}")
    print(f"Bronze rows: {metrics['bronze_rows']:,}")
    print(f"Quarantine rows: {metrics['quarantine_rows']:,}")
    print(f"Silver rows: {metrics['silver_rows']:,}")
    print(f"Gold daily rows: {metrics['gold_daily_rows']:,}")


if __name__ == "__main__":
    main()
