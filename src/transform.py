from __future__ import annotations

import argparse
from pathlib import Path

try:
    from .config import (
        BRONZE_TRANSACTIONS_DIR,
        GOLD_FRAUD_BY_TRANSACTION_TYPE_FILE,
        GOLD_FRAUD_SUMMARY_DAILY_FILE,
        GOLD_HIGH_RISK_ACCOUNTS_FILE,
        GOLD_HOURLY_FRAUD_TREND_FILE,
        GOLD_LARGE_TRANSACTION_ALERTS_FILE,
        SILVER_ACCOUNTS_FILE,
        SILVER_TRANSACTION_TYPES_FILE,
        SILVER_TRANSACTIONS_DIR,
        SQL_DIR,
    )
    from .db import (
        connect,
        count_relation,
        create_or_replace_parquet_view,
        remove_path,
        sql_string,
    )
    from .quality import create_quality_scored_view
except ImportError:  # pragma: no cover - supports running scripts directly
    from config import (
        BRONZE_TRANSACTIONS_DIR,
        GOLD_FRAUD_BY_TRANSACTION_TYPE_FILE,
        GOLD_FRAUD_SUMMARY_DAILY_FILE,
        GOLD_HIGH_RISK_ACCOUNTS_FILE,
        GOLD_HOURLY_FRAUD_TREND_FILE,
        GOLD_LARGE_TRANSACTION_ALERTS_FILE,
        SILVER_ACCOUNTS_FILE,
        SILVER_TRANSACTION_TYPES_FILE,
        SILVER_TRANSACTIONS_DIR,
        SQL_DIR,
    )
    from db import (
        connect,
        count_relation,
        create_or_replace_parquet_view,
        remove_path,
        sql_string,
    )
    from quality import create_quality_scored_view


def read_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text(encoding="utf-8").strip().rstrip(";")


def copy_query_to_parquet(
    con,
    query: str,
    output_path: str | Path,
    *,
    partition_by: str | None = None,
) -> None:
    output = Path(output_path)
    remove_path(output)
    output.parent.mkdir(parents=True, exist_ok=True)

    if partition_by:
        output.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"""
            COPY ({query})
            TO {sql_string(output)}
            (FORMAT PARQUET, PARTITION_BY ({partition_by}), OVERWRITE_OR_IGNORE TRUE)
            """
        )
    else:
        con.execute(
            f"""
            COPY ({query})
            TO {sql_string(output)}
            (FORMAT PARQUET)
            """
        )


def register_views(con) -> None:
    create_or_replace_parquet_view(
        con,
        "bronze_transactions",
        BRONZE_TRANSACTIONS_DIR,
        hive_partitioning=True,
    )
    create_or_replace_parquet_view(
        con,
        "silver_transactions",
        SILVER_TRANSACTIONS_DIR,
        hive_partitioning=True,
    )
    create_or_replace_parquet_view(con, "silver_accounts", SILVER_ACCOUNTS_FILE)
    create_or_replace_parquet_view(
        con,
        "silver_transaction_types",
        SILVER_TRANSACTION_TYPES_FILE,
    )
    create_or_replace_parquet_view(
        con,
        "gold_fraud_summary_daily",
        GOLD_FRAUD_SUMMARY_DAILY_FILE,
    )
    create_or_replace_parquet_view(
        con,
        "gold_fraud_by_transaction_type",
        GOLD_FRAUD_BY_TRANSACTION_TYPE_FILE,
    )
    create_or_replace_parquet_view(
        con,
        "gold_high_risk_accounts",
        GOLD_HIGH_RISK_ACCOUNTS_FILE,
    )
    create_or_replace_parquet_view(
        con,
        "gold_hourly_fraud_trend",
        GOLD_HOURLY_FRAUD_TREND_FILE,
    )
    create_or_replace_parquet_view(
        con,
        "gold_large_transaction_alerts",
        GOLD_LARGE_TRANSACTION_ALERTS_FILE,
    )


def run_transform(db_path: str | Path | None = None) -> dict[str, int]:
    con = connect(db_path) if db_path else connect()
    register_views(con)
    create_quality_scored_view(con)

    copy_query_to_parquet(
        con,
        read_sql("silver_transactions.sql"),
        SILVER_TRANSACTIONS_DIR,
        partition_by="transaction_day",
    )
    create_or_replace_parquet_view(
        con,
        "silver_transactions",
        SILVER_TRANSACTIONS_DIR,
        hive_partitioning=True,
    )

    copy_query_to_parquet(con, read_sql("silver_accounts.sql"), SILVER_ACCOUNTS_FILE)
    copy_query_to_parquet(
        con,
        read_sql("silver_transaction_types.sql"),
        SILVER_TRANSACTION_TYPES_FILE,
    )
    create_or_replace_parquet_view(con, "silver_accounts", SILVER_ACCOUNTS_FILE)
    create_or_replace_parquet_view(
        con,
        "silver_transaction_types",
        SILVER_TRANSACTION_TYPES_FILE,
    )

    copy_query_to_parquet(
        con,
        read_sql("gold_fraud_summary.sql"),
        GOLD_FRAUD_SUMMARY_DAILY_FILE,
    )
    copy_query_to_parquet(
        con,
        read_sql("gold_fraud_by_transaction_type.sql"),
        GOLD_FRAUD_BY_TRANSACTION_TYPE_FILE,
    )
    copy_query_to_parquet(
        con,
        read_sql("gold_high_risk_accounts.sql"),
        GOLD_HIGH_RISK_ACCOUNTS_FILE,
    )
    copy_query_to_parquet(
        con,
        read_sql("gold_hourly_fraud_trend.sql"),
        GOLD_HOURLY_FRAUD_TREND_FILE,
    )
    copy_query_to_parquet(
        con,
        read_sql("gold_large_transaction_alerts.sql"),
        GOLD_LARGE_TRANSACTION_ALERTS_FILE,
    )
    register_views(con)

    metrics = {
        "silver_rows": count_relation(con, "silver_transactions"),
        "gold_daily_rows": count_relation(con, "gold_fraud_summary_daily"),
    }
    con.close()
    return metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver and gold PayFlow tables.")
    parser.add_argument("--db-path", default=None, help="DuckDB metadata database path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    metrics = run_transform(args.db_path)
    print(
        "Transform complete: "
        f"{metrics['silver_rows']:,} silver rows, "
        f"{metrics['gold_daily_rows']:,} daily gold rows"
    )


if __name__ == "__main__":
    main()
