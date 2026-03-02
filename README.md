# Project 3 – Transaction Monitoring Engine (SQL)

## Objective
Design and implement a SQL-based transaction monitoring engine simulating rule-based AML detection through alert generation.

Customer risk attributes derived from Project 1 (KYC & Risk Profiling Engine) are integrated into transaction-level monitoring.

## Dataset
Synthetic AML monitoring environment comprising:
- Standardized multi-year transaction data
- Customer risk categories generated using the Project 1 risk scoring model
- FATF-aligned jurisdiction risk classifications
- Multi-account customer relationships enabling behavioural analysis

Calibrated to produce controlled, realistic alert-generation scenarios.

## Methodology
- Apply rule-driven monitoring logic using SQL
- Evaluate predefined detection thresholds on risk-enriched transaction data
- Structure monitoring scenarios through modular SQL CTE architecture
- Generate alert records when rule conditions are met

## Key Outputs
Monitoring alerts covering:
- High-risk customer activity (PEP/high-risk profiles)
- High-value and cumulative transactions
- High-risk jurisdiction exposure
- Cash aggregation activity
- Structuring and transaction velocity patterns

## Repository Contents
- project3_tm.sql 	– SQL monitoring engine
- project3_report.pdf 	– Final analytical report
- project3_report.docx – Editable analytical report
- /outputs/ 		– Generated datasets (consolidated dataset, alert results, analytical summaries)

## Limitations
- Synthetic dataset within a controlled AML simulation environment
- Rule-based monitoring architecture (no machine learning or adaptive behavioural models)
- No external intelligence integration (sanctions, adverse media, network analysis)
- Batch execution architecture (not real-time monitoring)

