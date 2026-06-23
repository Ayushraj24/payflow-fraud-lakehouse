from __future__ import annotations

import sys
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))

from config import (  # noqa: E402
    DB_PATH,
    EXPECTED_TRANSACTION_TYPES,
    GOLD_FRAUD_BY_TRANSACTION_TYPE_FILE,
    GOLD_FRAUD_SUMMARY_DAILY_FILE,
    GOLD_HIGH_RISK_ACCOUNTS_FILE,
    GOLD_HOURLY_FRAUD_TREND_FILE,
    GOLD_LARGE_TRANSACTION_ALERTS_FILE,
    SILVER_TRANSACTIONS_DIR,
)
from db import has_parquet, parquet_glob, sql_string  # noqa: E402

alt.data_transformers.disable_max_rows()

TABLE_PATHS = {
    "silver_transactions": {
        "path": SILVER_TRANSACTIONS_DIR,
        "hive_partitioning": True,
    },
    "gold_fraud_summary_daily": {
        "path": GOLD_FRAUD_SUMMARY_DAILY_FILE,
        "hive_partitioning": False,
    },
    "gold_fraud_by_transaction_type": {
        "path": GOLD_FRAUD_BY_TRANSACTION_TYPE_FILE,
        "hive_partitioning": False,
    },
    "gold_high_risk_accounts": {
        "path": GOLD_HIGH_RISK_ACCOUNTS_FILE,
        "hive_partitioning": False,
    },
    "gold_hourly_fraud_trend": {
        "path": GOLD_HOURLY_FRAUD_TREND_FILE,
        "hive_partitioning": False,
    },
    "gold_large_transaction_alerts": {
        "path": GOLD_LARGE_TRANSACTION_ALERTS_FILE,
        "hive_partitioning": False,
    },
}


def parquet_relation(table_name: str) -> str:
    spec = TABLE_PATHS[table_name]
    path = spec["path"]
    if not has_parquet(path):
        raise FileNotFoundError(f"Missing table output: {path}")

    hive_partitioning = "true" if spec["hive_partitioning"] else "false"
    return (
        f"read_parquet({sql_string(parquet_glob(path))}, "
        f"hive_partitioning = {hive_partitioning})"
    )


def sql_list(values: list[str]) -> str:
    return ", ".join(sql_string(value) for value in values)


def filtered_transactions_sql(
    selected_types: list[str],
    day_range: tuple[int, int],
) -> str:
    valid_types = [
        value for value in selected_types if value in EXPECTED_TRANSACTION_TYPES
    ]
    type_condition = (
        f"transaction_type IN ({sql_list(valid_types)})" if valid_types else "false"
    )
    start_day, end_day = day_range
    return f"""
        SELECT *
        FROM {parquet_relation("silver_transactions")}
        WHERE transaction_day BETWEEN {int(start_day)} AND {int(end_day)}
          AND {type_condition}
    """


@st.cache_data(ttl=300)
def query_df(sql: str) -> pd.DataFrame:
    con = duckdb.connect()
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


@st.cache_data(ttl=300)
def transaction_bounds() -> dict[str, int]:
    relation = parquet_relation("silver_transactions")
    df = query_df(
        f"""
        SELECT
            MIN(transaction_day)::INTEGER AS min_day,
            MAX(transaction_day)::INTEGER AS max_day
        FROM {relation}
        """
    )
    return {
        "min_day": int(df.loc[0, "min_day"]),
        "max_day": int(df.loc[0, "max_day"]),
    }


def table_df(table_name: str) -> pd.DataFrame:
    return query_df(f"SELECT * FROM {parquet_relation(table_name)}")


def money(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "$0"
    value = float(value)
    if abs(value) >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if abs(value) >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:,.0f}"


def whole(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "0"
    return f"{int(value):,}"


def pct(value: float | int | None, digits: int = 3) -> str:
    if value is None or pd.isna(value):
        return "0%"
    return f"{float(value):.{digits}f}%"


def metric_row(metrics: pd.Series) -> None:
    total_transactions = float(metrics["total_transactions"])
    fraud_transactions = float(metrics["fraud_transactions"])
    fraud_rate = (
        100 * fraud_transactions / total_transactions if total_transactions else 0
    )

    cols = st.columns(6)
    cols[0].metric("Transactions", whole(total_transactions))
    cols[1].metric("Fraud cases", whole(fraud_transactions))
    cols[2].metric("Fraud rate", pct(fraud_rate))
    cols[3].metric("Fraud amount", money(metrics["fraud_amount"]))
    cols[4].metric("P99 amount", money(metrics["p99_amount"]))
    cols[5].metric("Source flags", whole(metrics["source_flagged_transactions"]))


def overview_metrics(filtered_sql: str) -> pd.DataFrame:
    return query_df(
        f"""
        WITH filtered AS ({filtered_sql})
        SELECT
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_transactions,
            ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2) AS fraud_amount,
            ROUND(SUM(amount), 2) AS total_amount,
            quantile_cont(amount, 0.99) AS p99_amount,
            approx_count_distinct(sender_account) AS approx_senders,
            approx_count_distinct(receiver_account) AS approx_receivers,
            SUM(CASE WHEN is_flagged_fraud THEN 1 ELSE 0 END) AS source_flagged_transactions
        FROM filtered
        """
    )


def type_risk(filtered_sql: str) -> pd.DataFrame:
    return query_df(
        f"""
        WITH filtered AS ({filtered_sql}),
        baseline AS (
            SELECT AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END) AS baseline_rate
            FROM filtered
        ),
        grouped AS (
            SELECT
                transaction_type,
                COUNT(*) AS total_transactions,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_transactions,
                SUM(CASE WHEN is_fraud THEN amount ELSE 0 END) AS fraud_amount,
                SUM(amount) AS total_amount,
                AVG(amount) AS avg_amount,
                AVG(CASE WHEN is_fraud THEN amount ELSE NULL END) AS avg_fraud_amount,
                AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END) AS fraud_rate
            FROM filtered
            GROUP BY transaction_type
        )
        SELECT
            grouped.transaction_type,
            grouped.total_transactions,
            grouped.fraud_transactions,
            ROUND(100 * grouped.fraud_rate, 4) AS fraud_rate_pct,
            ROUND(grouped.fraud_rate / NULLIF(baseline.baseline_rate, 0), 2) AS fraud_lift,
            ROUND(grouped.fraud_transactions - baseline.baseline_rate * grouped.total_transactions, 2)
                AS excess_fraud_vs_baseline,
            ROUND(grouped.total_amount, 2) AS total_amount,
            ROUND(grouped.fraud_amount, 2) AS fraud_amount,
            ROUND(grouped.avg_amount, 2) AS avg_amount,
            ROUND(grouped.avg_fraud_amount, 2) AS avg_fraud_amount
        FROM grouped
        CROSS JOIN baseline
        ORDER BY fraud_lift DESC, fraud_amount DESC
        """
    )


def daily_trend(filtered_sql: str) -> pd.DataFrame:
    return query_df(
        f"""
        WITH filtered AS ({filtered_sql})
        SELECT
            transaction_day,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_transactions,
            ROUND(100 * AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END), 4)
                AS fraud_rate_pct,
            ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2) AS fraud_amount
        FROM filtered
        GROUP BY transaction_day
        ORDER BY transaction_day
        """
    )


def hourly_heatmap(filtered_sql: str) -> pd.DataFrame:
    return query_df(
        f"""
        WITH filtered AS ({filtered_sql})
        SELECT
            transaction_day,
            transaction_hour,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_transactions,
            ROUND(100 * AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END), 4)
                AS fraud_rate_pct,
            ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2) AS fraud_amount
        FROM filtered
        GROUP BY transaction_day, transaction_hour
        ORDER BY transaction_day, transaction_hour
        """
    )


def behavior_signals(filtered_sql: str) -> pd.DataFrame:
    return query_df(
        f"""
        WITH filtered AS ({filtered_sql}),
        thresholds AS (
            SELECT quantile_cont(amount, 0.99) AS p99_amount
            FROM filtered
        ),
        baseline AS (
            SELECT
                AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END) AS baseline_rate,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS total_fraud
            FROM filtered
        ),
        signals AS (
            SELECT
                'Sender drained to zero' AS signal,
                is_fraud,
                amount
            FROM filtered
            WHERE sender_old_balance > 0
              AND sender_new_balance = 0
              AND amount >= sender_old_balance * 0.95

            UNION ALL

            SELECT
                'Amount equals prior sender balance' AS signal,
                is_fraud,
                amount
            FROM filtered
            WHERE sender_old_balance > 0
              AND sender_new_balance = 0
              AND ABS(amount - sender_old_balance) <= 0.01

            UNION ALL

            SELECT
                'Top 1% amount' AS signal,
                filtered.is_fraud,
                filtered.amount
            FROM filtered
            CROSS JOIN thresholds
            WHERE filtered.amount >= thresholds.p99_amount

            UNION ALL

            SELECT
                'Receiver is customer account' AS signal,
                is_fraud,
                amount
            FROM filtered
            WHERE starts_with(receiver_account, 'C')

            UNION ALL

            SELECT
                'Receiver starts with zero balance' AS signal,
                is_fraud,
                amount
            FROM filtered
            WHERE receiver_old_balance = 0

            UNION ALL

            SELECT
                'Source isFlaggedFraud rule' AS signal,
                is_fraud,
                amount
            FROM filtered
            WHERE is_flagged_fraud
        )
        SELECT
            signals.signal,
            COUNT(*) AS signal_transactions,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS signal_frauds,
            ROUND(100 * AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END), 4)
                AS fraud_rate_pct,
            ROUND(
                AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END)
                / NULLIF(baseline.baseline_rate, 0),
                2
            ) AS fraud_lift,
            ROUND(
                100 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)
                / NULLIF(baseline.total_fraud, 0),
                2
            ) AS fraud_capture_pct,
            ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2) AS fraud_amount
        FROM signals
        CROSS JOIN baseline
        GROUP BY signals.signal, baseline.baseline_rate, baseline.total_fraud
        ORDER BY fraud_lift DESC, signal_frauds DESC
        """
    )


def amount_decile_risk(filtered_sql: str) -> pd.DataFrame:
    return query_df(
        f"""
        WITH filtered AS ({filtered_sql}),
        baseline AS (
            SELECT AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END) AS baseline_rate
            FROM filtered
        ),
        deciled AS (
            SELECT
                amount,
                is_fraud,
                NTILE(10) OVER (ORDER BY amount) AS amount_decile
            FROM filtered
        )
        SELECT
            amount_decile,
            COUNT(*) AS total_transactions,
            ROUND(MIN(amount), 2) AS min_amount,
            ROUND(MAX(amount), 2) AS max_amount,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_transactions,
            ROUND(100 * AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END), 4)
                AS fraud_rate_pct,
            ROUND(
                AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END)
                / NULLIF(baseline.baseline_rate, 0),
                2
            ) AS fraud_lift
        FROM deciled
        CROSS JOIN baseline
        GROUP BY amount_decile, baseline.baseline_rate
        ORDER BY amount_decile
        """
    )


def account_risk(filtered_sql: str) -> pd.DataFrame:
    return query_df(
        f"""
        WITH filtered AS ({filtered_sql}),
        account_events AS (
            SELECT
                sender_account AS account_id,
                'sender' AS account_role,
                amount,
                is_fraud
            FROM filtered

            UNION ALL

            SELECT
                receiver_account AS account_id,
                'receiver' AS account_role,
                amount,
                is_fraud
            FROM filtered
        ),
        account_metrics AS (
            SELECT
                account_id,
                COUNT(*) AS total_transactions_touched,
                SUM(CASE WHEN account_role = 'sender' THEN 1 ELSE 0 END)
                    AS sent_transactions,
                SUM(CASE WHEN account_role = 'receiver' THEN 1 ELSE 0 END)
                    AS received_transactions,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)
                    AS fraud_transactions_touched,
                ROUND(SUM(amount), 2) AS total_amount_touched,
                ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2)
                    AS fraud_amount_touched,
                ROUND(100 * AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END), 4)
                    AS fraud_touch_rate_pct
            FROM account_events
            GROUP BY account_id
        ),
        scored AS (
            SELECT
                *,
                ROUND(
                    fraud_transactions_touched * 40
                    + LEAST(fraud_amount_touched / 100000.0, 75)
                    + LEAST(fraud_touch_rate_pct * 2, 50)
                    + CASE WHEN sent_transactions = 0 THEN 5 ELSE 0 END,
                    2
                ) AS risk_score
            FROM account_metrics
            WHERE fraud_transactions_touched > 0
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (ORDER BY risk_score DESC, fraud_amount_touched DESC) AS risk_rank,
                SUM(fraud_amount_touched) OVER (
                    ORDER BY fraud_amount_touched DESC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_fraud_amount,
                SUM(fraud_amount_touched) OVER () AS total_fraud_amount
            FROM scored
        )
        SELECT
            risk_rank,
            account_id,
            total_transactions_touched,
            sent_transactions,
            received_transactions,
            fraud_transactions_touched,
            total_amount_touched,
            fraud_amount_touched,
            fraud_touch_rate_pct,
            risk_score,
            CASE
                WHEN risk_score >= 120 THEN 'CRITICAL'
                WHEN risk_score >= 80 THEN 'HIGH'
                WHEN risk_score >= 40 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS risk_band,
            ROUND(100 * cumulative_fraud_amount / NULLIF(total_fraud_amount, 0), 2)
                AS cumulative_loss_pct
        FROM ranked
        ORDER BY risk_score DESC, fraud_amount_touched DESC
        LIMIT 250
        """
    )


def source_flag_performance(filtered_sql: str) -> pd.DataFrame:
    return query_df(
        f"""
        WITH filtered AS ({filtered_sql}),
        counts AS (
            SELECT
                SUM(CASE WHEN is_flagged_fraud THEN 1 ELSE 0 END) AS alerts,
                SUM(CASE WHEN is_flagged_fraud AND is_fraud THEN 1 ELSE 0 END)
                    AS true_positives,
                SUM(CASE WHEN is_flagged_fraud AND NOT is_fraud THEN 1 ELSE 0 END)
                    AS false_positives,
                SUM(CASE WHEN NOT is_flagged_fraud AND is_fraud THEN 1 ELSE 0 END)
                    AS false_negatives,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS all_frauds,
                ROUND(SUM(CASE WHEN is_flagged_fraud AND is_fraud THEN amount ELSE 0 END), 2)
                    AS fraud_amount_captured,
                ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2)
                    AS total_fraud_amount
            FROM filtered
        )
        SELECT
            alerts,
            true_positives,
            false_positives,
            false_negatives,
            all_frauds,
            ROUND(100 * true_positives / NULLIF(alerts, 0), 2) AS precision_pct,
            ROUND(100 * true_positives / NULLIF(all_frauds, 0), 2) AS recall_pct,
            ROUND(
                100 * 2
                * (true_positives / NULLIF(alerts, 0))
                * (true_positives / NULLIF(all_frauds, 0))
                / NULLIF(
                    (true_positives / NULLIF(alerts, 0))
                    + (true_positives / NULLIF(all_frauds, 0)),
                    0
                ),
                2
            ) AS f1_pct,
            fraud_amount_captured,
            total_fraud_amount,
            ROUND(100 * fraud_amount_captured / NULLIF(total_fraud_amount, 0), 2)
                AS fraud_amount_recall_pct
        FROM counts
        """
    )


def rule_performance(
    filtered_sql: str,
    selected_types: list[str],
    percentile: float,
    require_sender_drained: bool,
    require_customer_receiver: bool,
) -> pd.DataFrame:
    valid_types = [
        value for value in selected_types if value in EXPECTED_TRANSACTION_TYPES
    ]
    type_condition = (
        f"transaction_type IN ({sql_list(valid_types)})" if valid_types else "false"
    )
    drained_condition = (
        """
        AND sender_old_balance > 0
        AND sender_new_balance = 0
        AND amount >= sender_old_balance * 0.95
        """
        if require_sender_drained
        else ""
    )
    receiver_condition = (
        "AND starts_with(receiver_account, 'C')" if require_customer_receiver else ""
    )

    return query_df(
        f"""
        WITH filtered AS ({filtered_sql}),
        thresholds AS (
            SELECT quantile_cont(amount, {float(percentile)}) AS amount_threshold
            FROM filtered
        ),
        scored AS (
            SELECT
                filtered.*,
                thresholds.amount_threshold,
                CASE
                    WHEN {type_condition}
                     AND amount >= thresholds.amount_threshold
                     {drained_condition}
                     {receiver_condition}
                    THEN true
                    ELSE false
                END AS alert
            FROM filtered
            CROSS JOIN thresholds
        ),
        counts AS (
            SELECT
                MAX(amount_threshold) AS amount_threshold,
                SUM(CASE WHEN alert THEN 1 ELSE 0 END) AS alerts,
                SUM(CASE WHEN alert AND is_fraud THEN 1 ELSE 0 END) AS true_positives,
                SUM(CASE WHEN alert AND NOT is_fraud THEN 1 ELSE 0 END) AS false_positives,
                SUM(CASE WHEN NOT alert AND is_fraud THEN 1 ELSE 0 END) AS false_negatives,
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS all_frauds,
                ROUND(SUM(CASE WHEN alert AND is_fraud THEN amount ELSE 0 END), 2)
                    AS fraud_amount_captured,
                ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2)
                    AS total_fraud_amount
            FROM scored
        )
        SELECT
            amount_threshold,
            alerts,
            true_positives,
            false_positives,
            false_negatives,
            all_frauds,
            ROUND(100 * true_positives / NULLIF(alerts, 0), 2) AS precision_pct,
            ROUND(100 * true_positives / NULLIF(all_frauds, 0), 2) AS recall_pct,
            ROUND(
                100 * 2
                * (true_positives / NULLIF(alerts, 0))
                * (true_positives / NULLIF(all_frauds, 0))
                / NULLIF(
                    (true_positives / NULLIF(alerts, 0))
                    + (true_positives / NULLIF(all_frauds, 0)),
                    0
                ),
                2
            ) AS f1_pct,
            fraud_amount_captured,
            total_fraud_amount,
            ROUND(100 * fraud_amount_captured / NULLIF(total_fraud_amount, 0), 2)
                AS fraud_amount_recall_pct
        FROM counts
        """
    )


def large_alerts(filtered_sql: str) -> pd.DataFrame:
    return query_df(
        f"""
        WITH filtered AS ({filtered_sql}),
        thresholds AS (
            SELECT
                quantile_cont(amount, 0.99) AS p99_amount,
                quantile_cont(amount, 0.995) AS p995_amount
            FROM filtered
        ),
        scored AS (
            SELECT
                transaction_id,
                transaction_day,
                transaction_hour,
                transaction_type,
                amount,
                sender_account,
                receiver_account,
                sender_old_balance,
                sender_new_balance,
                receiver_old_balance,
                receiver_new_balance,
                is_fraud,
                is_flagged_fraud,
                CASE
                    WHEN amount >= thresholds.p995_amount THEN 35 ELSE 0
                END
                + CASE
                    WHEN sender_old_balance > 0
                     AND sender_new_balance = 0
                     AND amount >= sender_old_balance * 0.95
                    THEN 25 ELSE 0
                END
                + CASE
                    WHEN transaction_type IN ('TRANSFER', 'CASH_OUT')
                    THEN 20 ELSE 0
                END
                + CASE WHEN starts_with(receiver_account, 'C') THEN 10 ELSE 0 END
                + CASE WHEN is_flagged_fraud THEN 30 ELSE 0 END AS alert_score,
                CASE
                    WHEN is_flagged_fraud THEN 'source flagged'
                    WHEN amount >= thresholds.p995_amount THEN 'top 0.5% amount'
                    WHEN sender_old_balance > 0
                     AND sender_new_balance = 0
                     AND amount >= sender_old_balance * 0.95
                    THEN 'sender drained'
                    ELSE 'watchlist'
                END AS primary_reason
            FROM filtered
            CROSS JOIN thresholds
        )
        SELECT *
        FROM scored
        WHERE alert_score >= 55
        ORDER BY alert_score DESC, amount DESC
        LIMIT 500
        """
    )


def show_overview(filtered_sql: str) -> None:
    metrics = overview_metrics(filtered_sql)
    if metrics.empty or int(metrics.loc[0, "total_transactions"]) == 0:
        st.warning("No transactions match the selected filters.")
        return

    metric_row(metrics.loc[0])
    type_df = type_risk(filtered_sql)
    heat_df = hourly_heatmap(filtered_sql)
    trend_df = daily_trend(filtered_sql)

    st.subheader("Fraud risk by transaction type")
    bubble = (
        alt.Chart(type_df)
        .mark_circle(opacity=0.78)
        .encode(
            x=alt.X("total_transactions:Q", title="Transaction volume"),
            y=alt.Y("fraud_rate_pct:Q", title="Fraud rate (%)"),
            size=alt.Size("fraud_amount:Q", title="Fraud amount", scale=alt.Scale(range=[250, 2200])),
            color=alt.Color("transaction_type:N", title="Type"),
            tooltip=[
                "transaction_type:N",
                alt.Tooltip("total_transactions:Q", format=","),
                alt.Tooltip("fraud_transactions:Q", format=","),
                alt.Tooltip("fraud_rate_pct:Q", format=".4f"),
                alt.Tooltip("fraud_lift:Q", format=".2f"),
                alt.Tooltip("excess_fraud_vs_baseline:Q", format=",.2f"),
                alt.Tooltip("fraud_amount:Q", format=",.2f"),
            ],
        )
        .properties(height=340)
    )
    st.altair_chart(bubble, use_container_width=True)

    left, right = st.columns([1.15, 1])
    with left:
        st.subheader("Day and hour fraud heatmap")
        heatmap = (
            alt.Chart(heat_df)
            .mark_rect()
            .encode(
                x=alt.X("transaction_hour:O", title="Hour"),
                y=alt.Y("transaction_day:O", title="Day"),
                color=alt.Color(
                    "fraud_rate_pct:Q",
                    title="Fraud rate (%)",
                    scale=alt.Scale(scheme="reds"),
                ),
                tooltip=[
                    "transaction_day:O",
                    "transaction_hour:O",
                    alt.Tooltip("total_transactions:Q", format=","),
                    alt.Tooltip("fraud_transactions:Q", format=","),
                    alt.Tooltip("fraud_rate_pct:Q", format=".4f"),
                    alt.Tooltip("fraud_amount:Q", format=",.2f"),
                ],
            )
            .properties(height=360)
        )
        st.altair_chart(heatmap, use_container_width=True)

    with right:
        st.subheader("Daily fraud rate trend")
        trend = (
            alt.Chart(trend_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("transaction_day:O", title="Day"),
                y=alt.Y("fraud_rate_pct:Q", title="Fraud rate (%)"),
                tooltip=[
                    "transaction_day:O",
                    alt.Tooltip("total_transactions:Q", format=","),
                    alt.Tooltip("fraud_transactions:Q", format=","),
                    alt.Tooltip("fraud_rate_pct:Q", format=".4f"),
                    alt.Tooltip("fraud_amount:Q", format=",.2f"),
                ],
            )
            .properties(height=360)
        )
        st.altair_chart(trend, use_container_width=True)

    st.dataframe(
        type_df,
        use_container_width=True,
        hide_index=True,
    )


def show_fraud_drivers(filtered_sql: str) -> None:
    signals = behavior_signals(filtered_sql)
    deciles = amount_decile_risk(filtered_sql)

    left, right = st.columns([1.1, 1])
    with left:
        st.subheader("Behavior signals ranked by fraud lift")
        signal_chart = (
            alt.Chart(signals)
            .mark_bar(cornerRadiusEnd=3)
            .encode(
                x=alt.X("fraud_lift:Q", title="Lift over baseline"),
                y=alt.Y("signal:N", title=None, sort="-x"),
                color=alt.Color(
                    "fraud_capture_pct:Q",
                    title="Fraud captured (%)",
                    scale=alt.Scale(scheme="orangered"),
                ),
                tooltip=[
                    "signal:N",
                    alt.Tooltip("signal_transactions:Q", format=","),
                    alt.Tooltip("signal_frauds:Q", format=","),
                    alt.Tooltip("fraud_rate_pct:Q", format=".4f"),
                    alt.Tooltip("fraud_lift:Q", format=".2f"),
                    alt.Tooltip("fraud_capture_pct:Q", format=".2f"),
                    alt.Tooltip("fraud_amount:Q", format=",.2f"),
                ],
            )
            .properties(height=340)
        )
        st.altair_chart(signal_chart, use_container_width=True)

    with right:
        st.subheader("Amount decile risk curve")
        decile_chart = (
            alt.Chart(deciles)
            .mark_bar()
            .encode(
                x=alt.X("amount_decile:O", title="Amount decile"),
                y=alt.Y("fraud_rate_pct:Q", title="Fraud rate (%)"),
                color=alt.Color(
                    "fraud_lift:Q",
                    title="Lift",
                    scale=alt.Scale(scheme="blues"),
                ),
                tooltip=[
                    "amount_decile:O",
                    alt.Tooltip("min_amount:Q", format=",.2f"),
                    alt.Tooltip("max_amount:Q", format=",.2f"),
                    alt.Tooltip("total_transactions:Q", format=","),
                    alt.Tooltip("fraud_transactions:Q", format=","),
                    alt.Tooltip("fraud_rate_pct:Q", format=".4f"),
                    alt.Tooltip("fraud_lift:Q", format=".2f"),
                ],
            )
            .properties(height=340)
        )
        st.altair_chart(decile_chart, use_container_width=True)

    st.subheader("Signal detail")
    st.dataframe(signals, use_container_width=True, hide_index=True)

    st.subheader("Amount bands")
    st.dataframe(deciles, use_container_width=True, hide_index=True)


def show_account_risk(filtered_sql: str) -> None:
    accounts = account_risk(filtered_sql)
    if accounts.empty:
        st.warning("No risky accounts match the selected filters.")
        return

    top_accounts = accounts.head(30).copy()
    left, right = st.columns([1.2, 1])

    with left:
        st.subheader("High-risk account ranking")
        rank_chart = (
            alt.Chart(top_accounts)
            .mark_bar(cornerRadiusEnd=3)
            .encode(
                x=alt.X("risk_score:Q", title="Risk score"),
                y=alt.Y("account_id:N", sort="-x", title=None),
                color=alt.Color(
                    "risk_band:N",
                    title="Risk band",
                    scale=alt.Scale(
                        domain=["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                        range=["#b91c1c", "#ef4444", "#f59e0b", "#22c55e"],
                    ),
                ),
                tooltip=[
                    "account_id:N",
                    alt.Tooltip("risk_score:Q", format=".2f"),
                    "risk_band:N",
                    alt.Tooltip("fraud_transactions_touched:Q", format=","),
                    alt.Tooltip("fraud_amount_touched:Q", format=",.2f"),
                    alt.Tooltip("fraud_touch_rate_pct:Q", format=".4f"),
                ],
            )
            .properties(height=520)
        )
        st.altair_chart(rank_chart, use_container_width=True)

    with right:
        st.subheader("Loss concentration")
        pareto_accounts = accounts.sort_values("fraud_amount_touched", ascending=False).head(50)
        bars = (
            alt.Chart(pareto_accounts)
            .mark_bar(opacity=0.75)
            .encode(
                x=alt.X("account_id:N", sort=None, axis=alt.Axis(labels=False), title="Top accounts by fraud amount"),
                y=alt.Y("fraud_amount_touched:Q", title="Fraud amount"),
                tooltip=[
                    "account_id:N",
                    alt.Tooltip("fraud_amount_touched:Q", format=",.2f"),
                    alt.Tooltip("cumulative_loss_pct:Q", format=".2f"),
                ],
            )
        )
        line = (
            alt.Chart(pareto_accounts)
            .mark_line(color="#f97316", point=True)
            .encode(
                x=alt.X("account_id:N", sort=None, axis=alt.Axis(labels=False)),
                y=alt.Y("cumulative_loss_pct:Q", title="Cumulative loss (%)"),
                tooltip=[
                    "account_id:N",
                    alt.Tooltip("cumulative_loss_pct:Q", format=".2f"),
                ],
            )
        )
        st.altair_chart(
            alt.layer(bars, line).resolve_scale(y="independent").properties(height=520),
            use_container_width=True,
        )

    st.subheader("Account risk table")
    st.dataframe(accounts, use_container_width=True, hide_index=True)


def performance_metrics(df: pd.DataFrame, title: str) -> None:
    if df.empty:
        st.warning(f"No performance metrics available for {title}.")
        return

    row = df.loc[0]
    st.subheader(title)
    cols = st.columns(6)
    cols[0].metric("Alerts", whole(row["alerts"]))
    cols[1].metric("True frauds", whole(row["true_positives"]))
    cols[2].metric("Precision", pct(row["precision_pct"], 2))
    cols[3].metric("Recall", pct(row["recall_pct"], 2))
    cols[4].metric("F1", pct(row["f1_pct"], 2))
    cols[5].metric("Fraud $ captured", money(row["fraud_amount_captured"]))

    confusion = pd.DataFrame(
        {
            "outcome": ["True positives", "False positives", "False negatives"],
            "transactions": [
                row["true_positives"],
                row["false_positives"],
                row["false_negatives"],
            ],
        }
    )
    chart = (
        alt.Chart(confusion)
        .mark_bar(cornerRadiusEnd=3)
        .encode(
            x=alt.X("transactions:Q", title="Transactions"),
            y=alt.Y("outcome:N", sort="-x", title=None),
            color=alt.Color(
                "outcome:N",
                legend=None,
                scale=alt.Scale(
                    domain=["True positives", "False positives", "False negatives"],
                    range=["#22c55e", "#f59e0b", "#ef4444"],
                ),
            ),
            tooltip=[
                "outcome:N",
                alt.Tooltip("transactions:Q", format=","),
            ],
        )
        .properties(height=170)
    )
    st.altair_chart(chart, use_container_width=True)


def show_detection_lab(filtered_sql: str) -> None:
    source_perf = source_flag_performance(filtered_sql)
    performance_metrics(source_perf, "PaySim source flag performance")

    st.subheader("Rule simulator")
    controls = st.columns(4)
    rule_types = controls[0].multiselect(
        "Rule transaction types",
        list(EXPECTED_TRANSACTION_TYPES),
        default=["TRANSFER", "CASH_OUT"],
    )
    percentile_label = controls[1].selectbox(
        "Minimum amount percentile",
        ["90%", "95%", "99%", "99.5%"],
        index=2,
    )
    percentile = {
        "90%": 0.90,
        "95%": 0.95,
        "99%": 0.99,
        "99.5%": 0.995,
    }[percentile_label]
    require_sender_drained = controls[2].checkbox(
        "Require sender drained",
        value=True,
    )
    require_customer_receiver = controls[3].checkbox(
        "Require customer receiver",
        value=False,
    )

    rule_perf = rule_performance(
        filtered_sql,
        rule_types,
        percentile,
        require_sender_drained,
        require_customer_receiver,
    )
    if not rule_perf.empty:
        threshold = rule_perf.loc[0, "amount_threshold"]
        st.caption(f"Current amount threshold: {money(threshold)}")
    performance_metrics(rule_perf, "Simulated alert rule performance")

    st.subheader("Highest-scoring suspicious transactions")
    alerts = large_alerts(filtered_sql)
    st.dataframe(alerts, use_container_width=True, hide_index=True)


def show_pipeline_state() -> None:
    if DB_PATH.exists():
        con = duckdb.connect(str(DB_PATH), read_only=True)
        try:
            runs = con.execute(
                """
                SELECT
                    run_id,
                    started_at,
                    finished_at,
                    status,
                    bronze_rows,
                    quarantine_rows,
                    silver_rows,
                    gold_daily_rows
                FROM pipeline_runs
                ORDER BY started_at DESC
                LIMIT 10
                """
            ).fetchdf()
            st.subheader("Recent pipeline runs")
            st.dataframe(runs, use_container_width=True, hide_index=True)

            quality = con.execute(
                """
                SELECT
                    check_name,
                    failed_rows,
                    total_rows,
                    status
                FROM quality_results
                WHERE quality_run_id = (
                    SELECT quality_run_id
                    FROM quality_results
                    WHERE quality_run_id IS NOT NULL
                    ORDER BY checked_at DESC
                    LIMIT 1
                )
                ORDER BY check_name
                """
            ).fetchdf()
            if not quality.empty:
                st.subheader("Latest quality results")
                st.dataframe(quality, use_container_width=True, hide_index=True)
        finally:
            con.close()
    else:
        st.info("No pipeline run history found yet.")


def sidebar_filters() -> tuple[list[str], tuple[int, int]]:
    bounds = transaction_bounds()
    st.sidebar.header("Controls")
    selected_types = st.sidebar.multiselect(
        "Transaction types",
        list(EXPECTED_TRANSACTION_TYPES),
        default=list(EXPECTED_TRANSACTION_TYPES),
    )
    day_range = st.sidebar.slider(
        "Transaction day",
        min_value=bounds["min_day"],
        max_value=bounds["max_day"],
        value=(bounds["min_day"], bounds["max_day"]),
    )
    return selected_types, day_range


def main() -> None:
    st.set_page_config(page_title="PayFlow Fraud Analytics", layout="wide")
    st.title("PayFlow Fraud Analytics")

    missing = [
        name for name, spec in TABLE_PATHS.items() if not has_parquet(spec["path"])
    ]
    if missing:
        st.warning("Lakehouse tables are missing. Run `python src/run_pipeline.py` first.")
        st.caption(", ".join(missing))
        return

    selected_types, day_range = sidebar_filters()
    filtered_sql = filtered_transactions_sql(selected_types, day_range)

    page = st.sidebar.radio(
        "View",
        [
            "Executive Overview",
            "Fraud Drivers",
            "Account Risk",
            "Detection Lab",
            "Pipeline Runs",
        ],
    )

    if page == "Executive Overview":
        show_overview(filtered_sql)
    elif page == "Fraud Drivers":
        show_fraud_drivers(filtered_sql)
    elif page == "Account Risk":
        show_account_risk(filtered_sql)
    elif page == "Detection Lab":
        show_detection_lab(filtered_sql)
    else:
        show_pipeline_state()


if __name__ == "__main__":
    main()
