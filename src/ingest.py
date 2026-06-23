from __future__ import annotations

import argparse
from pathlib import Path

try:
    from .config import BRONZE_TRANSACTIONS_DIR, resolve_raw_file
    from .db import connect, count_relation, create_or_replace_parquet_view, remove_path, sql_string
except ImportError:  # pragma: no cover - supports running scripts directly
    from config import BRONZE_TRANSACTIONS_DIR, resolve_raw_file
    from db import connect, count_relation, create_or_replace_parquet_view, remove_path, sql_string


def bronze_select_sql(raw_file: Path, sample_rows: int | None = None) -> str:
    limit_clause = f"LIMIT {int(sample_rows)}" if sample_rows else ""
    return f"""
        WITH raw_csv AS (
            SELECT *
            FROM read_csv_auto(
                {sql_string(raw_file)},
                header = true,
                sample_size = -1
            )
            {limit_clause}
        )
        SELECT
            row_number() OVER () AS source_row_number,
            *,
            CAST(CEIL(TRY_CAST(step AS DOUBLE) / 24.0) AS INTEGER) AS step_day,
            current_timestamp AS ingested_at,
            {sql_string(raw_file.name)} AS source_file
        FROM raw_csv
    """


def run_ingest(
    raw_file: str | Path | None = None,
    sample_rows: int | None = None,
    db_path: str | Path | None = None,
) -> dict[str, int | str]:
    source_file = resolve_raw_file(raw_file)
    con = connect(db_path) if db_path else connect()

    remove_path(BRONZE_TRANSACTIONS_DIR)
    BRONZE_TRANSACTIONS_DIR.mkdir(parents=True, exist_ok=True)

    con.execute(
        f"""
        COPY ({bronze_select_sql(source_file, sample_rows)})
        TO {sql_string(BRONZE_TRANSACTIONS_DIR)}
        (FORMAT PARQUET, PARTITION_BY (step_day), OVERWRITE_OR_IGNORE TRUE)
        """
    )
    create_or_replace_parquet_view(
        con,
        "bronze_transactions",
        BRONZE_TRANSACTIONS_DIR,
        hive_partitioning=True,
    )

    rows = count_relation(con, "bronze_transactions")
    con.close()
    return {"raw_file": str(source_file), "bronze_rows": rows}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest PaySim CSV into bronze Parquet.")
    parser.add_argument("--raw-file", default=None, help="Path to the PaySim CSV.")
    parser.add_argument("--sample-rows", type=int, default=None, help="Limit rows for a smoke run.")
    parser.add_argument("--db-path", default=None, help="DuckDB metadata database path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    metrics = run_ingest(args.raw_file, args.sample_rows, args.db_path)
    print(f"Ingested {metrics['bronze_rows']:,} rows from {metrics['raw_file']}")


if __name__ == "__main__":
    main()
