# Functional Requirements Document

## Project: PayFlow Fraud Analytics Lakehouse

## 1. Purpose

This Functional Requirements Document defines what the PayFlow Fraud Analytics Lakehouse must do from a system and user-functionality perspective. It translates the business requirements into concrete pipeline, data quality, transformation, dashboard, testing, and documentation requirements.

## 2. System Overview

The system is a local batch-processing lakehouse built with Python, DuckDB, Parquet, SQL, pytest, and Streamlit.

High-level flow:

```text
PaySim CSV
  -> Raw layer
  -> Bronze Parquet
  -> Quality checks and quarantine
  -> Silver clean tables
  -> Gold analytics tables
  -> Streamlit dashboard
```

## 3. Users

| User | Functional Need |
| --- | --- |
| Data engineer | Run and maintain the ingestion, quality, and transformation pipeline |
| Fraud analyst | Explore fraud KPIs, risk drivers, suspicious transactions, and risky accounts |
| Risk manager | Review high-level fraud trends and alert-rule performance |
| Interviewer or reviewer | Understand project structure, design choices, and output quality |

## 4. Functional Requirements Summary

| ID | Requirement |
| --- | --- |
| FR-001 | Ingest PaySim CSV into bronze Parquet |
| FR-002 | Partition bronze data by transaction day |
| FR-003 | Run quality checks on bronze transactions |
| FR-004 | Write bad records to quarantine |
| FR-005 | Build clean silver transaction table |
| FR-006 | Build silver account and transaction-type tables |
| FR-007 | Build gold fraud analytics tables |
| FR-008 | Store pipeline run metadata |
| FR-009 | Provide a dashboard for fraud analytics |
| FR-010 | Provide filters and rule simulator controls |
| FR-011 | Provide tests for quality logic and project structure |
| FR-012 | Provide documentation for setup, execution, and explanation |

## 5. Data Ingestion Requirements

### FR-001: CSV Ingestion

The system must read the PaySim CSV file from:

```text
data/raw/PS_20174392719_1491204439457_log.csv
```

The ingestion process must:

- Use DuckDB to read the CSV.
- Preserve original source columns.
- Add ingestion metadata.
- Add a derived transaction day field.
- Write output to the bronze layer.

Implemented by:

```text
src/ingest.py
```

### FR-002: Bronze Parquet Storage

The system must write bronze data to:

```text
data/bronze/bronze_transactions/
```

The bronze output must:

- Use Parquet format.
- Be partitioned by `step_day`.
- Be queryable by DuckDB.
- Represent the loaded source data before cleaning.

## 6. Data Quality Requirements

### FR-003: Quality Checks

The system must validate bronze transactions using the following checks:

| Check | Failure condition |
| --- | --- |
| Positive amount | `amount <= 0` or amount cannot be parsed |
| Valid transaction type | Type is not one of the expected PaySim values |
| Sender present | Sender account is null or blank |
| Receiver present | Receiver account is null or blank |
| Valid fraud flag | `isFraud` is not `0` or `1` |
| Valid source flag | `isFlaggedFraud` is not `0` or `1` |
| Non-negative balances | Any balance field is null or negative |
| Duplicate transaction | Same transaction values occur more than once |

Implemented by:

```text
src/quality.py
```

### FR-004: Quarantine

The system must write records that fail quality checks to:

```text
data/quarantine/quarantine_bad_transactions.parquet
```

Quarantined records must include:

- Original transaction fields.
- Parsed validation fields.
- `quality_issues`.
- Quarantine timestamp.

Bad records must not be included in `silver_transactions`.

### FR-005: Quality Results Metadata

The system must store quality result summaries in `pipeline_runs.duckdb`.

The `quality_results` table must include:

- Quality run ID.
- Check timestamp.
- Check name.
- Failed row count.
- Total row count.
- Pass/fail status.

## 7. Transformation Requirements

### FR-006: Silver Transactions

The system must create:

```text
data/silver/silver_transactions/
```

The silver transaction table must:

- Include only records with no quality issues.
- Deduplicate repeated transactions.
- Normalize transaction type.
- Cast amount, balance, step, and flag fields to appropriate types.
- Add `transaction_id`.
- Add `transaction_day`.
- Add `transaction_hour`.
- Keep sender and receiver account fields.

SQL model:

```text
sql/silver_transactions.sql
```

### FR-007: Silver Accounts

The system must create:

```text
data/silver/silver_accounts.parquet
```

The silver account table must calculate:

- First seen step.
- Last seen step.
- Sent transaction count.
- Received transaction count.
- Total transactions touched.
- Total amount touched.
- Fraud transactions touched.
- Fraud amount touched.
- Fraud touch rate.

SQL model:

```text
sql/silver_accounts.sql
```

### FR-008: Silver Transaction Types

The system must create:

```text
data/silver/silver_transaction_types.parquet
```

The table must calculate:

- Total transactions by type.
- Fraud transactions by type.
- Fraud rate by type.
- Total amount by type.
- Fraud amount by type.
- Average transaction amount.

SQL model:

```text
sql/silver_transaction_types.sql
```

## 8. Gold Table Requirements

### FR-009: Daily Fraud Summary

The system must create:

```text
data/gold/gold_fraud_summary_daily.parquet
```

Required metrics:

- Transaction day.
- Total transactions.
- Fraud transactions.
- Fraud rate.
- Total amount.
- Fraud amount.
- Source flagged transactions.
- Average fraud amount.

### FR-010: Fraud by Transaction Type

The system must create:

```text
data/gold/gold_fraud_by_transaction_type.parquet
```

Required metrics:

- Transaction type.
- Total transactions.
- Fraud transactions.
- Fraud rate.
- Total amount.
- Fraud amount.
- Average fraud amount.

### FR-011: High Risk Accounts

The system must create:

```text
data/gold/gold_high_risk_accounts.parquet
```

Required fields:

- Account ID.
- Total transactions touched.
- Sent and received transaction counts.
- Fraud transactions touched.
- Fraud amount touched.
- Average fraud amount.
- Risk score.
- Risk band.

### FR-012: Hourly Fraud Trend

The system must create:

```text
data/gold/gold_hourly_fraud_trend.parquet
```

Required metrics:

- Transaction hour.
- Total transactions.
- Fraud transactions.
- Fraud rate.
- Fraud amount.

### FR-013: Large Transaction Alerts

The system must create:

```text
data/gold/gold_large_transaction_alerts.parquet
```

Required fields:

- Transaction ID.
- Day and hour.
- Transaction type.
- Amount.
- Sender and receiver account.
- Fraud flag.
- Source flagged fraud flag.
- Alert reason.
- Alert score.

## 9. Pipeline Orchestration Requirements

### FR-014: Full Pipeline Runner

The system must provide one command to run ingestion, quality checks, and transformations:

```bash
python src/run_pipeline.py
```

The system must also support sample runs:

```bash
python src/run_pipeline.py --sample-rows 100000
```

Implemented by:

```text
src/run_pipeline.py
```

### FR-015: Pipeline Run History

The system must write run metadata to:

```text
pipeline_runs.duckdb
```

The `pipeline_runs` table must include:

- Run ID.
- Start timestamp.
- Finish timestamp.
- Status.
- Raw file path.
- Sample row count.
- Bronze row count.
- Quarantine row count.
- Silver row count.
- Gold daily row count.
- Notes.

## 10. Dashboard Requirements

### FR-016: Dashboard Launch

The system must provide a Streamlit dashboard launched with:

```bash
streamlit run dashboard/app.py
```

The dashboard must query existing Parquet outputs and should not require rerunning the pipeline for normal usage.

### FR-017: Global Dashboard Filters

The dashboard must provide:

- Transaction type filter.
- Transaction day range filter.

These filters must affect analytics pages that query silver transaction data.

### FR-018: Executive Overview Page

The dashboard must show:

- Total transactions.
- Fraud cases.
- Fraud rate.
- Fraud amount.
- P99 transaction amount.
- Source flag count.
- Transaction-type risk bubble chart.
- Day/hour fraud heatmap.
- Daily fraud-rate trend.
- Transaction-type detail table.

### FR-019: Fraud Drivers Page

The dashboard must show:

- Behavior signals ranked by fraud lift.
- Amount-decile risk curve.
- Signal detail table.
- Amount band table.

Behavior signals must include:

- Sender drained to zero.
- Amount equals prior sender balance.
- Top 1% amount.
- Receiver is customer account.
- Receiver starts with zero balance.
- Source `isFlaggedFraud` rule.

### FR-020: Account Risk Page

The dashboard must show:

- High-risk account ranking.
- Account risk score.
- Risk band.
- Fraud amount touched.
- Fraud touch rate.
- Loss concentration chart.
- Account risk table.

### FR-021: Detection Lab Page

The dashboard must show source flag performance:

- Alerts.
- True positives.
- False positives.
- False negatives.
- Precision.
- Recall.
- F1 score.
- Fraud amount captured.

The dashboard must also provide a configurable rule simulator with:

- Rule transaction types.
- Amount percentile threshold.
- Option to require sender drained.
- Option to require customer receiver.

The simulator must output:

- Alert count.
- True positives.
- Precision.
- Recall.
- F1 score.
- Fraud amount captured.
- Highest-scoring suspicious transactions.

### FR-022: Pipeline Runs Page

The dashboard must show:

- Recent pipeline runs.
- Latest quality results.

## 11. Calculation Requirements

The system must calculate the following metrics.

Fraud rate:

```text
fraud_transactions / total_transactions
```

Fraud lift:

```text
segment_fraud_rate / overall_fraud_rate
```

Excess fraud vs baseline:

```text
actual_fraud_count - expected_fraud_count_at_baseline_rate
```

Precision:

```text
true_positives / alerts
```

Recall:

```text
true_positives / all_frauds
```

F1:

```text
2 * precision * recall / (precision + recall)
```

Fraud amount recall:

```text
fraud_amount_captured_by_rule / total_fraud_amount
```

## 12. Testing Requirements

The project must include pytest tests.

Required tests:

| Test file | Purpose |
| --- | --- |
| `tests/test_project_structure.py` | Confirms required scripts and SQL models exist |
| `tests/test_quality_rules.py` | Confirms quality logic catches invalid records and duplicates |

Tests must be runnable with:

```bash
pytest
```

## 13. Non-Functional Requirements

| Category | Requirement |
| --- | --- |
| Performance | Full dataset should run locally with DuckDB |
| Portability | Project should run using a local Python virtual environment |
| Maintainability | SQL models should live in the `sql/` folder |
| Reproducibility | README must explain setup, pipeline execution, tests, and dashboard launch |
| Usability | Dashboard must provide business-readable metrics and visualizations |
| Data trust | Bad records must be quarantined rather than silently ignored |

## 14. Acceptance Criteria

The project is complete when:

- The PaySim CSV can be ingested from `data/raw`.
- Bronze Parquet is created.
- Quality checks run successfully.
- Bad records are written to quarantine.
- Silver and gold tables are created.
- Pipeline run metadata is stored.
- Streamlit dashboard loads successfully.
- Dashboard includes fraud drivers, account risk, and rule evaluation.
- Tests pass with `pytest`.
- README, BRD, and FRD explain the project clearly.

## 15. Current Implementation Status

| Requirement Area | Status |
| --- | --- |
| CSV ingestion | Complete |
| Bronze Parquet | Complete |
| Quality checks | Complete |
| Quarantine | Complete |
| Silver tables | Complete |
| Gold tables | Complete |
| Pipeline metadata | Complete |
| Streamlit dashboard | Complete |
| Advanced fraud analytics | Complete |
| Rule simulator | Complete |
| Tests | Complete |
| Documentation | Complete |
