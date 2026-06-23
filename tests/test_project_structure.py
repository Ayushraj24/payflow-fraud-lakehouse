from pathlib import Path


def test_required_sql_models_exist():
    root = Path(__file__).resolve().parents[1]
    required_files = [
        "sql/silver_transactions.sql",
        "sql/gold_fraud_summary.sql",
        "sql/gold_high_risk_accounts.sql",
        "src/ingest.py",
        "src/quality.py",
        "src/transform.py",
        "src/run_pipeline.py",
        "dashboard/app.py",
    ]

    for relative_path in required_files:
        assert (root / relative_path).exists(), relative_path
