WITH account_events AS (
    SELECT
        sender_account AS account_id,
        'sender' AS account_role,
        amount,
        is_fraud
    FROM silver_transactions

    UNION ALL

    SELECT
        receiver_account AS account_id,
        'receiver' AS account_role,
        amount,
        is_fraud
    FROM silver_transactions
),
account_metrics AS (
    SELECT
        account_id,
        COUNT(*) AS total_transactions_touched,
        SUM(CASE WHEN account_role = 'sender' THEN 1 ELSE 0 END) AS sent_transactions,
        SUM(CASE WHEN account_role = 'receiver' THEN 1 ELSE 0 END) AS received_transactions,
        SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_transactions_touched,
        ROUND(SUM(amount), 2) AS total_amount_touched,
        ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2) AS fraud_amount_touched,
        ROUND(AVG(CASE WHEN is_fraud THEN amount ELSE NULL END), 2) AS avg_fraud_amount
    FROM account_events
    GROUP BY account_id
),
scored AS (
    SELECT
        *,
        ROUND(
            fraud_transactions_touched * 50
            + LEAST(fraud_amount_touched / 10000.0, 50)
            + LEAST(total_transactions_touched / 1000.0, 20),
            2
        ) AS risk_score
    FROM account_metrics
)
SELECT
    account_id,
    total_transactions_touched,
    sent_transactions,
    received_transactions,
    fraud_transactions_touched,
    total_amount_touched,
    fraud_amount_touched,
    avg_fraud_amount,
    risk_score,
    CASE
        WHEN risk_score >= 100 THEN 'CRITICAL'
        WHEN risk_score >= 60 THEN 'HIGH'
        WHEN risk_score >= 25 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_band
FROM scored
WHERE fraud_transactions_touched > 0
ORDER BY risk_score DESC, fraud_amount_touched DESC
LIMIT 1000
