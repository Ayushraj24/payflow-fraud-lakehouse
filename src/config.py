from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
QUARANTINE_DIR = DATA_DIR / "quarantine"
SQL_DIR = PROJECT_ROOT / "sql"
SCREENSHOTS_DIR = PROJECT_ROOT / "screenshots"

DB_PATH = PROJECT_ROOT / "pipeline_runs.duckdb"

BRONZE_TRANSACTIONS_DIR = BRONZE_DIR / "bronze_transactions"
SILVER_TRANSACTIONS_DIR = SILVER_DIR / "silver_transactions"
SILVER_ACCOUNTS_FILE = SILVER_DIR / "silver_accounts.parquet"
SILVER_TRANSACTION_TYPES_FILE = SILVER_DIR / "silver_transaction_types.parquet"
QUARANTINE_BAD_TRANSACTIONS_FILE = QUARANTINE_DIR / "quarantine_bad_transactions.parquet"

GOLD_FRAUD_SUMMARY_DAILY_FILE = GOLD_DIR / "gold_fraud_summary_daily.parquet"
GOLD_FRAUD_BY_TRANSACTION_TYPE_FILE = GOLD_DIR / "gold_fraud_by_transaction_type.parquet"
GOLD_HIGH_RISK_ACCOUNTS_FILE = GOLD_DIR / "gold_high_risk_accounts.parquet"
GOLD_HOURLY_FRAUD_TREND_FILE = GOLD_DIR / "gold_hourly_fraud_trend.parquet"
GOLD_LARGE_TRANSACTION_ALERTS_FILE = GOLD_DIR / "gold_large_transaction_alerts.parquet"

EXPECTED_TRANSACTION_TYPES = ("CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER")


def ensure_directories() -> None:
    for directory in (
        RAW_DIR,
        BRONZE_DIR,
        SILVER_DIR,
        GOLD_DIR,
        QUARANTINE_DIR,
        SQL_DIR,
        SCREENSHOTS_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)


def resolve_project_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = PROJECT_ROOT / resolved
    return resolved


def resolve_raw_file(raw_file: str | Path | None = None) -> Path:
    if raw_file:
        candidate = resolve_project_path(raw_file)
        if candidate.exists():
            return candidate
        raise FileNotFoundError(f"Raw file not found: {candidate}")

    csv_files = sorted(RAW_DIR.glob("*.csv"))
    if not csv_files:
        csv_files = sorted(DATA_DIR.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            "No PaySim CSV found. Put the Kaggle CSV under data/raw/ and rerun the pipeline."
        )

    return csv_files[0]
