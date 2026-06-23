# Business Requirements Document

## Project: PayFlow Fraud Analytics Lakehouse

## 1. Purpose

The purpose of this project is to build a local fraud analytics lakehouse for mobile-money transactions. The solution helps business and risk teams understand fraud patterns, monitor high-risk accounts, evaluate alert rules, and produce trusted fraud metrics from raw transaction data.

This is a portfolio-grade data engineering project that demonstrates how raw financial transaction data can be converted into clean, validated, analytics-ready data products.

## 2. Business Problem

Mobile-money platforms process large volumes of transactions across payment, cash-in, cash-out, debit, and transfer flows. Fraud can be hidden inside this transaction volume, especially when accounts are drained, unusually large transactions occur, or specific transaction types show higher fraud rates than normal.

The business needs a repeatable local pipeline that can:

- Ingest raw transaction data.
- Validate and quarantine bad records.
- Create trusted analytical tables.
- Highlight fraud trends and risk drivers.
- Identify accounts that need investigation.
- Compare source fraud flags against custom detection rules.

## 3. Business Objectives

| Objective | Description |
| --- | --- |
| Improve fraud visibility | Provide clear KPIs for fraud count, fraud rate, fraud amount, and high-risk transaction types |
| Build trusted analytics data | Separate raw, bronze, silver, and gold layers so analysis is based on clean data |
| Detect risky behavior | Identify behavior signals such as sender balance drain, large amount bands, and risky transaction types |
| Prioritize investigations | Rank high-risk accounts using fraud activity, fraud amount, and risk scoring |
| Evaluate alert quality | Show precision, recall, F1, and fraud amount captured by fraud rules |
| Support interview explanation | Present a project that demonstrates practical data engineering, SQL modeling, and analytics thinking |

## 4. Scope

### In Scope

- Use the PaySim synthetic mobile-money fraud dataset.
- Build a local lakehouse using DuckDB and Parquet.
- Implement raw, bronze, silver, gold, quarantine, and metadata layers.
- Run data quality checks before analytical transformations.
- Quarantine invalid transaction rows.
- Build fraud analytics tables.
- Create a Streamlit dashboard with advanced fraud metrics and visualizations.
- Store pipeline run history.
- Add unit tests for core project behavior.
- Provide documentation for setup, architecture, and business explanation.

### Out of Scope

- Real-time streaming ingestion.
- Production cloud deployment.
- Real customer data.
- User authentication and role-based access control.
- Enterprise alert case management.
- Bank-grade compliance workflows.
- Machine learning model deployment.

## 5. Stakeholders

| Stakeholder | Interest |
| --- | --- |
| Fraud analyst | Wants to understand fraud patterns and high-risk accounts |
| Risk manager | Wants summary metrics, trends, and rule performance |
| Data engineer | Owns ingestion, validation, transformation, and storage layers |
| Data analyst | Uses clean silver and gold tables for reporting |
| Interviewer or reviewer | Evaluates the project for data engineering fundamentals |

## 6. Business Questions Answered

The project should help answer:

- How many total transactions were processed?
- How many transactions were fraudulent?
- What is the overall fraud rate?
- What is the total fraud amount?
- Which transaction types have the highest fraud lift?
- Which days and hours show higher fraud concentration?
- Which account behaviors are strong fraud signals?
- Which amount bands show disproportionate fraud risk?
- Which accounts should be investigated first?
- How well does the original `isFlaggedFraud` source rule perform?
- Can a custom rule capture more fraud value?

## 7. Success Metrics

| Metric | Target |
| --- | --- |
| Pipeline completion | Full PaySim dataset can be processed locally |
| Data quality handling | Bad rows are detected and written to quarantine |
| Layered architecture | Raw, bronze, silver, gold, and quarantine layers are present |
| Dashboard usability | Dashboard explains fraud patterns beyond basic totals |
| Rule evaluation | Precision, recall, F1, and fraud amount recall are visible |
| Reproducibility | Project can be rerun using documented commands |
| Test coverage | Core quality-rule behavior is covered by pytest |

## 8. Business Rules

| Rule | Description |
| --- | --- |
| Valid amount | Transaction amount must be greater than zero |
| Valid transaction type | Type must be one of `CASH_IN`, `CASH_OUT`, `DEBIT`, `PAYMENT`, `TRANSFER` |
| Required sender | Sender account cannot be null or blank |
| Required receiver | Receiver account cannot be null or blank |
| Valid fraud label | `isFraud` must be `0` or `1` |
| Valid source flag | `isFlaggedFraud` must be `0` or `1` |
| Valid balances | Balance columns cannot be null or negative |
| Duplicate detection | Duplicate transaction rows should be detected |
| Quarantine handling | Bad records should be excluded from silver and written to quarantine |

## 9. Key Business Metrics

| Metric | Meaning |
| --- | --- |
| Total transactions | Count of clean transactions processed |
| Fraud transactions | Count of transactions labeled fraud |
| Fraud rate | Fraud transactions divided by total transactions |
| Fraud amount | Sum of amount for fraud transactions |
| Fraud lift | Segment fraud rate divided by overall fraud rate |
| Excess fraud vs baseline | Actual fraud count minus expected fraud count at baseline rate |
| Precision | Share of alerts that are true fraud |
| Recall | Share of all fraud cases caught by a rule |
| F1 score | Balanced measure of precision and recall |
| Fraud amount recall | Share of fraud value captured by a rule |
| Risk score | Account score based on fraud activity and fraud amount |

## 10. Assumptions

- PaySim is synthetic and safe to use for local development.
- The CSV file is already downloaded into `data/raw`.
- DuckDB can process the dataset on a local machine.
- Batch processing is acceptable for this project.
- Fraud labels in PaySim are treated as ground truth for analytics.
- Dashboard users are technical or semi-technical users comfortable with fraud metrics.

## 11. Constraints

- The solution runs locally, not in a cloud data platform.
- The dataset is historical and static.
- Streamlit is used for dashboarding rather than an enterprise BI tool.
- DuckDB is used as the processing engine rather than Spark.
- The project prioritizes clarity and interview readiness over production complexity.

## 12. Risks

| Risk | Mitigation |
| --- | --- |
| Large CSV processing may be slow | DuckDB scans CSV and writes Parquet for faster repeat analytics |
| Bad records may distort metrics | Quality checks quarantine invalid records before silver/gold transformations |
| Dashboard may become basic EDA | Dashboard includes lift, rule performance, heatmaps, risk scoring, and behavior signals |
| Source fraud flag may be misleading | Detection Lab measures precision, recall, F1, and fraud amount recall |
| Project may be hard to explain | README, BRD, and FRD separate business, technical, and functional explanations |

## 13. Expected Business Outcome

The final project should demonstrate a realistic fraud analytics workflow:

1. Raw financial transaction data is ingested.
2. Bad data is detected and isolated.
3. Clean analytical tables are produced.
4. Business-facing fraud metrics are calculated.
5. Risk patterns and account priorities are visualized.
6. Fraud detection rules are evaluated with measurable outcomes.

## 14. Resume Summary

Built a local DuckDB lakehouse for mobile-money fraud analytics using 6M synthetic transactions. Implemented raw, bronze, silver, and gold layers, Parquet storage, data quality checks, quarantine handling, reusable SQL transformations, pipeline run metadata, and a Streamlit dashboard for fraud KPIs, risk drivers, high-risk accounts, and alert-rule evaluation.
