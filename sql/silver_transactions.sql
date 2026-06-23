SELECT
    md5(concat_ws(
        '|',
        COALESCE(CAST(parsed_step AS VARCHAR), ''),
        COALESCE(normalized_transaction_type, ''),
        COALESCE(CAST(parsed_amount AS VARCHAR), ''),
        COALESCE(sender_account, ''),
        COALESCE(receiver_account, ''),
        COALESCE(CAST(parsed_oldbalanceOrg AS VARCHAR), ''),
        COALESCE(CAST(parsed_newbalanceOrig AS VARCHAR), ''),
        COALESCE(CAST(parsed_oldbalanceDest AS VARCHAR), ''),
        COALESCE(CAST(parsed_newbalanceDest AS VARCHAR), ''),
        COALESCE(CAST(parsed_isFraud AS VARCHAR), ''),
        COALESCE(CAST(parsed_isFlaggedFraud AS VARCHAR), '')
    )) AS transaction_id,
    parsed_step AS transaction_step,
    CAST(CEIL(parsed_step / 24.0) AS INTEGER) AS transaction_day,
    CAST(((parsed_step - 1) % 24) AS INTEGER) AS transaction_hour,
    normalized_transaction_type AS transaction_type,
    CAST(parsed_amount AS DOUBLE) AS amount,
    sender_account,
    receiver_account,
    CAST(parsed_oldbalanceOrg AS DOUBLE) AS sender_old_balance,
    CAST(parsed_newbalanceOrig AS DOUBLE) AS sender_new_balance,
    CAST(parsed_oldbalanceDest AS DOUBLE) AS receiver_old_balance,
    CAST(parsed_newbalanceDest AS DOUBLE) AS receiver_new_balance,
    CAST(parsed_isFraud AS BOOLEAN) AS is_fraud,
    CAST(parsed_isFlaggedFraud AS BOOLEAN) AS is_flagged_fraud,
    source_row_number,
    ingested_at,
    source_file
FROM quality_scored_transactions
WHERE quality_issues = ''
  AND duplicate_rank = 1
