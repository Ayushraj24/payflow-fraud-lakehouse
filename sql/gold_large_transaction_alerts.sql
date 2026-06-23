WITH thresholds AS (
    SELECT
        quantile_cont(amount, 0.990) AS p99_amount,
        quantile_cont(amount, 0.995) AS p995_amount
    FROM silver_transactions
),
alerts AS (
    SELECT
        st.transaction_id,
        st.transaction_step,
        st.transaction_day,
        st.transaction_hour,
        st.transaction_type,
        st.amount,
        st.sender_account,
        st.receiver_account,
        st.is_fraud,
        st.is_flagged_fraud,
        thresholds.p99_amount,
        thresholds.p995_amount,
        CASE
            WHEN st.is_flagged_fraud THEN 'source_flagged_fraud'
            WHEN st.is_fraud AND st.amount >= thresholds.p99_amount THEN 'known_fraud_large_amount'
            WHEN st.amount >= thresholds.p995_amount THEN 'large_amount_outlier'
            ELSE 'watchlist_large_transaction'
        END AS alert_reason,
        ROUND(
            CASE WHEN st.is_fraud THEN 70 ELSE 0 END
            + CASE WHEN st.is_flagged_fraud THEN 30 ELSE 0 END
            + LEAST(st.amount / NULLIF(thresholds.p995_amount, 0) * 20, 20),
            2
        ) AS alert_score
    FROM silver_transactions st
    CROSS JOIN thresholds
    WHERE st.amount >= thresholds.p995_amount
       OR st.is_flagged_fraud
       OR (st.is_fraud AND st.amount >= thresholds.p99_amount)
)
SELECT *
FROM alerts
ORDER BY alert_score DESC, amount DESC
LIMIT 500
