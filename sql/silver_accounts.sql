WITH account_events AS (
    SELECT
        sender_account AS account_id,
        'sender' AS account_role,
        transaction_step,
        amount,
        is_fraud
    FROM silver_transactions

    UNION ALL

    SELECT
        receiver_account AS account_id,
        'receiver' AS account_role,
        transaction_step,
        amount,
        is_fraud
    FROM silver_transactions
)
SELECT
    account_id,
    substr(account_id, 1, 1) AS account_prefix,
    MIN(transaction_step) AS first_seen_step,
    MAX(transaction_step) AS last_seen_step,
    COUNT(*) AS total_transactions_touched,
    SUM(CASE WHEN account_role = 'sender' THEN 1 ELSE 0 END) AS sent_transactions,
    SUM(CASE WHEN account_role = 'receiver' THEN 1 ELSE 0 END) AS received_transactions,
    ROUND(SUM(amount), 2) AS total_amount_touched,
    ROUND(SUM(CASE WHEN account_role = 'sender' THEN amount ELSE 0 END), 2) AS total_sent_amount,
    ROUND(SUM(CASE WHEN account_role = 'receiver' THEN amount ELSE 0 END), 2) AS total_received_amount,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_transactions_touched,
    ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2) AS fraud_amount_touched,
    ROUND(
        100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        4
    ) AS fraud_touch_rate_pct
FROM account_events
GROUP BY account_id
