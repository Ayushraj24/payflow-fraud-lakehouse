from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

try:
    from .config import (
        BRONZE_TRANSACTIONS_DIR,
        EXPECTED_TRANSACTION_TYPES,
        QUARANTINE_BAD_TRANSACTIONS_FILE,
    )
    from .db import (
        connect,
        count_relation,
        create_or_replace_parquet_view,
        init_metadata,
        remove_path,
        sql_string,
    )
except ImportError:  # pragma: no cover - supports running scripts directly
    from config import (
        BRONZE_TRANSACTIONS_DIR,
        EXPECTED_TRANSACTION_TYPES,
        QUARANTINE_BAD_TRANSACTIONS_FILE,
    )
    from db import (
        connect,
        count_relation,
        create_or_replace_parquet_view,
        init_metadata,
        remove_path,
        sql_string,
    )

QUALITY_CHECKS = {
    "amount_positive": "amount_not_positive",
    "valid_transaction_type": "invalid_transaction_type",
    "sender_present": "missing_sender_account",
    "receiver_present": "missing_receiver_account",
    "valid_fraud_flag": "invalid_fraud_flag",
    "valid_flagged_fraud_flag": "invalid_flagged_fraud_flag",
    "non_negative_balances": "negative_or_null_balance",
    "duplicate_transaction": "duplicate_transaction",
}


def expected_types_sql() -> str:
    return ", ".join(sql_string(value) for value in EXPECTED_TRANSACTION_TYPES)


def duplicate_key_sql() -> str:
    fields = (
        "step",
        "type",
        "amount",
        "nameOrig",
        "oldbalanceOrg",
        "newbalanceOrig",
        "nameDest",
        "oldbalanceDest",
        "newbalanceDest",
        "isFraud",
        "isFlaggedFraud",
    )
    return ", ".join(f"COALESCE(CAST({field} AS VARCHAR), '')" for field in fields)


def create_quality_scored_view(
    con,
    source_relation: str = "bronze_transactions",
    view_name: str = "quality_scored_transactions",
) -> None:
    duplicate_key = duplicate_key_sql()
    expected_types = expected_types_sql()
    con.execute(
        f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH parsed AS (
            SELECT
                b.*,
                TRY_CAST(step AS INTEGER) AS parsed_step,
                UPPER(TRIM(CAST(type AS VARCHAR))) AS normalized_transaction_type,
                TRY_CAST(amount AS DOUBLE) AS parsed_amount,
                NULLIF(TRIM(CAST(nameOrig AS VARCHAR)), '') AS sender_account,
                NULLIF(TRIM(CAST(nameDest AS VARCHAR)), '') AS receiver_account,
                TRY_CAST(oldbalanceOrg AS DOUBLE) AS parsed_oldbalanceOrg,
                TRY_CAST(newbalanceOrig AS DOUBLE) AS parsed_newbalanceOrig,
                TRY_CAST(oldbalanceDest AS DOUBLE) AS parsed_oldbalanceDest,
                TRY_CAST(newbalanceDest AS DOUBLE) AS parsed_newbalanceDest,
                TRY_CAST(isFraud AS INTEGER) AS parsed_isFraud,
                TRY_CAST(isFlaggedFraud AS INTEGER) AS parsed_isFlaggedFraud,
                COUNT(*) OVER (PARTITION BY {duplicate_key}) AS duplicate_count,
                ROW_NUMBER() OVER (
                    PARTITION BY {duplicate_key}
                    ORDER BY COALESCE(TRY_CAST(source_row_number AS BIGINT), 9223372036854775807)
                ) AS duplicate_rank
            FROM {source_relation} b
        )
        SELECT
            *,
            TRIM(BOTH ',' FROM CONCAT(
                CASE
                    WHEN parsed_amount IS NULL OR parsed_amount <= 0
                    THEN 'amount_not_positive,' ELSE '' END,
                CASE
                    WHEN normalized_transaction_type IS NULL
                        OR normalized_transaction_type NOT IN ({expected_types})
                    THEN 'invalid_transaction_type,' ELSE '' END,
                CASE
                    WHEN sender_account IS NULL
                    THEN 'missing_sender_account,' ELSE '' END,
                CASE
                    WHEN receiver_account IS NULL
                    THEN 'missing_receiver_account,' ELSE '' END,
                CASE
                    WHEN parsed_isFraud IS NULL OR parsed_isFraud NOT IN (0, 1)
                    THEN 'invalid_fraud_flag,' ELSE '' END,
                CASE
                    WHEN parsed_isFlaggedFraud IS NULL OR parsed_isFlaggedFraud NOT IN (0, 1)
                    THEN 'invalid_flagged_fraud_flag,' ELSE '' END,
                CASE
                    WHEN parsed_oldbalanceOrg IS NULL OR parsed_oldbalanceOrg < 0
                        OR parsed_newbalanceOrig IS NULL OR parsed_newbalanceOrig < 0
                        OR parsed_oldbalanceDest IS NULL OR parsed_oldbalanceDest < 0
                        OR parsed_newbalanceDest IS NULL OR parsed_newbalanceDest < 0
                    THEN 'negative_or_null_balance,' ELSE '' END,
                CASE
                    WHEN duplicate_rank > 1
                    THEN 'duplicate_transaction,' ELSE '' END
            )) AS quality_issues
        FROM parsed
        """
    )


def run_quality(db_path: str | Path | None = None) -> dict[str, int]:
    con = connect(db_path) if db_path else connect()
    init_metadata(con)
    create_or_replace_parquet_view(
        con,
        "bronze_transactions",
        BRONZE_TRANSACTIONS_DIR,
        hive_partitioning=True,
    )
    create_quality_scored_view(con)

    total_rows = count_relation(con, "quality_scored_transactions")
    bad_rows = int(
        con.execute(
            "SELECT COUNT(*) FROM quality_scored_transactions WHERE quality_issues <> ''"
        ).fetchone()[0]
    )

    remove_path(QUARANTINE_BAD_TRANSACTIONS_FILE)
    con.execute(
        f"""
        COPY (
            SELECT
                *,
                current_timestamp AS quarantined_at
            FROM quality_scored_transactions
            WHERE quality_issues <> ''
        )
        TO {sql_string(QUARANTINE_BAD_TRANSACTIONS_FILE)}
        (FORMAT PARQUET)
        """
    )
    create_or_replace_parquet_view(
        con,
        "quarantine_bad_transactions",
        QUARANTINE_BAD_TRANSACTIONS_FILE,
    )

    quality_run_id = str(uuid4())
    checked_at = datetime.now(timezone.utc).replace(tzinfo=None)
    for check_name, issue_token in QUALITY_CHECKS.items():
        failed_rows = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM quality_scored_transactions
                WHERE contains(quality_issues, ?)
                """,
                [issue_token],
            ).fetchone()[0]
        )
        con.execute(
            """
            INSERT INTO quality_results (
                quality_run_id,
                checked_at,
                check_name,
                failed_rows,
                total_rows,
                status
            )
            VALUES (
                ?,
                ?,
                ?,
                ?,
                ?,
                CASE WHEN ? = 0 THEN 'PASS' ELSE 'FAIL' END
            )
            """,
            [quality_run_id, checked_at, check_name, failed_rows, total_rows, failed_rows],
        )

    con.close()
    return {"total_rows": total_rows, "quarantine_rows": bad_rows}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run PayFlow data quality checks.")
    parser.add_argument("--db-path", default=None, help="DuckDB metadata database path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    metrics = run_quality(args.db_path)
    print(
        "Quality checks complete: "
        f"{metrics['quarantine_rows']:,} bad rows out of {metrics['total_rows']:,}"
    )


if __name__ == "__main__":
    main()
