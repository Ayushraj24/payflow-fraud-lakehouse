import duckdb

from src.quality import create_quality_scored_view


def test_quality_scored_view_flags_bad_records_and_duplicates():
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE bronze_transactions (
            source_row_number INTEGER,
            step INTEGER,
            type VARCHAR,
            amount DOUBLE,
            nameOrig VARCHAR,
            oldbalanceOrg DOUBLE,
            newbalanceOrig DOUBLE,
            nameDest VARCHAR,
            oldbalanceDest DOUBLE,
            newbalanceDest DOUBLE,
            isFraud INTEGER,
            isFlaggedFraud INTEGER,
            step_day INTEGER,
            ingested_at TIMESTAMP,
            source_file VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO bronze_transactions VALUES
        (1, 1, 'PAYMENT', 100.0, 'C1', 200.0, 100.0, 'M1', 0.0, 0.0, 0, 0, 1, current_timestamp, 'test.csv'),
        (2, 1, 'WIRE', -5.0, '', -1.0, 0.0, NULL, 0.0, 0.0, 2, 0, 1, current_timestamp, 'test.csv'),
        (3, 2, 'TRANSFER', 50.0, 'C2', 50.0, 0.0, 'C3', 0.0, 50.0, 1, 0, 1, current_timestamp, 'test.csv'),
        (4, 2, 'TRANSFER', 50.0, 'C2', 50.0, 0.0, 'C3', 0.0, 50.0, 1, 0, 1, current_timestamp, 'test.csv')
        """
    )

    create_quality_scored_view(con)

    issues = dict(
        con.execute(
            """
            SELECT source_row_number, quality_issues
            FROM quality_scored_transactions
            ORDER BY source_row_number
            """
        ).fetchall()
    )

    assert issues[1] == ""
    assert "amount_not_positive" in issues[2]
    assert "invalid_transaction_type" in issues[2]
    assert "missing_sender_account" in issues[2]
    assert "missing_receiver_account" in issues[2]
    assert "invalid_fraud_flag" in issues[2]
    assert "negative_or_null_balance" in issues[2]
    assert issues[3] == ""
    assert "duplicate_transaction" in issues[4]
