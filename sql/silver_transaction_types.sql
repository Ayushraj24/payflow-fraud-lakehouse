SELECT
    transaction_type,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_transactions,
    ROUND(
        100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        4
    ) AS fraud_rate_pct,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2) AS fraud_amount,
    ROUND(AVG(amount), 2) AS average_amount
FROM silver_transactions
GROUP BY transaction_type
ORDER BY fraud_rate_pct DESC, fraud_transactions DESC
