/* ================================================================
   Project 3: AML Transaction Monitoring Scenarios
   Author: Anh Nguyen
   Date: 2026-02-17
   Database: SQLite (Simulated AML Transaction Monitoring Environment)

   Description:
   - Implements transaction monitoring scenarios aligned with
     common AML typologies (PEP monitoring, high-value activity,
     high-risk jurisdictions, cash aggregation, structuring,
     and wire velocity).

   Execution Note:
   - Run the entire file once to create analytical views.
   - Monitoring queries (Q1–Q7) are independent scenarios and
     may be executed separately.
   - Thresholds are simplified for simulation purposes and do
     not represent actual regulatory reporting limits.
   - Currency conversion uses fixed reference rates for
     analytical consistency within the project dataset.
   ================================================================ */


/* ================================================================
   SECTION A: Data Preparation & Normalisation Views
   ================================================================ */

-- ================================================================
-- MASTER BLOCK 1: Transaction Cleaning & EUR Conversion
-- Purpose: Standardize transaction date format and
--          normalize transaction amounts into EUR
-- ================================================================

CREATE VIEW vw_clean_transactions
AS
WITH clean_date
AS (
	SELECT tx_id
		,account_id
		,customer_id
		,tx_currency
		,tx_amount
		,tx_type
		,tx_country
		,is_cash
		,
		-- Convert MM/DD/YYYY → YYYY-MM-DD (SQLite date format)
		DATE (substr(tx_date, - 4) || '-' || printf('%02d', substr(tx_date, 1, instr(tx_date, '/') - 1)) || '-' || printf('%02d', substr(tx_date, instr(tx_date, '/') + 1, instr(substr(tx_date, instr(tx_date, '/') + 1), '/') - 1))) AS clean_tx_date
	FROM transactions
	)
	,eur_currency
AS (
	SELECT *
		,CASE 
			WHEN tx_currency = 'EUR'
				THEN tx_amount
			WHEN tx_currency = 'USD'
				THEN tx_amount * 0.86
			WHEN tx_currency = 'CHF'
				THEN tx_amount * 1.07
			END AS tx_amount_eur
	FROM clean_date
	)
SELECT tx_id
	,account_id
	,customer_id
	,clean_tx_date
	,tx_amount_eur
	,tx_currency
	,tx_type
	,tx_country
	,is_cash
FROM eur_currency;



/* ================================================================
   SECTION B: Customer Risk Context
   ================================================================ */

-- ================================================================
-- MASTER BLOCK 2: Customer Risk Profiling
-- Purpose: Derive customer risk category using
--          FATF-style scoring indicators
-- ================================================================

CREATE VIEW vw_customer_risk_profile
AS
WITH summary
AS (
	SELECT c.customer_id
		,c.full_name
		,c.pep_status
		,c.product_type
		,c.country AS customer_country
		,cr.risk_level AS customer_country_risk_level
		,MAX(t.tx_amount_eur) AS max_tx_amount_eur
		,AVG(t.tx_amount_eur) AS avg_tx_amount_eur
	FROM customers c
	INNER JOIN vw_clean_transactions t ON c.customer_id = t.customer_id
	INNER JOIN country_risk cr ON c.country = cr.country
	GROUP BY c.customer_id
		,c.full_name
		,c.pep_status
		,c.product_type
		,c.country
		,cr.risk_level
	)
	,risk_scoring
AS (
	SELECT *
		,CASE 
			WHEN pep_status = 'Y'
				THEN 3
			ELSE 0
			END AS pep_score
		,CASE customer_country_risk_level
			WHEN 'High'
				THEN 3
			WHEN 'Medium'
				THEN 1
			ELSE 0
			END AS customer_country_risk_score
		,CASE 
			WHEN max_tx_amount_eur >= 20000
				THEN 3
			WHEN max_tx_amount_eur >= 10000
				THEN 1
			ELSE 0
			END AS single_tx_score
		,CASE 
			WHEN avg_tx_amount_eur >= 15000
				THEN 2
			WHEN avg_tx_amount_eur >= 5000
				THEN 1
			ELSE 0
			END AS avg_tx_score
	FROM summary
	)
	,final_risk_scoring
AS (
	SELECT *
		,(pep_score + customer_country_risk_score + single_tx_score + avg_tx_score) AS final_risk_score
	FROM risk_scoring
	)
	,risk_categorizing
AS (
	SELECT *
		,CASE 
			WHEN final_risk_score >= 8
				THEN 'High'
			WHEN final_risk_score >= 4
				THEN 'Medium'
			ELSE 'Low'
			END AS customer_risk_category
	FROM final_risk_scoring
	)
SELECT customer_id
	,full_name
	,pep_status
	,product_type
	,customer_country
	,customer_country_risk_level
	,max_tx_amount_eur
	,avg_tx_amount_eur
	,final_risk_score
	,customer_risk_category
FROM risk_categorizing;


-- ================================================================
-- MASTER BLOCK 3: Customer Transaction Data Pool
-- Purpose: Combine cleaned transactions with 
--			customer risk profile and jurisdiction risk ratings 
--			for monitoring rule execution
-- ================================================================

CREATE VIEW vw_customer_data
AS
SELECT c.customer_id
	,t.account_id
	,c.full_name
	,c.pep_status
	,c.product_type
	,c.customer_country
	,c.customer_country_risk_level
	,c.max_tx_amount_eur
	,c.avg_tx_amount_eur
	,c.final_risk_score
	,c.customer_risk_category
	,t.tx_id
	,t.clean_tx_date
	,t.tx_amount_eur
	,t.tx_type
	,t.is_cash
	,t.tx_country
	,cr.risk_level AS tx_country_risk_level
FROM vw_clean_transactions t
INNER JOIN vw_customer_risk_profile c ON c.customer_id = t.customer_id
INNER JOIN country_risk cr ON t.tx_country = cr.country;



/* ================================================================
   SECTION C: AML Monitoring Scenarios
   ================================================================ */

-- ================================================================
-- Q1. Customer Risk Distribution
-- Purpose: Validate portfolio risk composition based on
--          FATF-style customer risk scoring
-- ================================================================

SELECT customer_risk_category
	,COUNT(*) AS total_customers
	,ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM vw_customer_risk_profile
GROUP BY customer_risk_category
ORDER BY CASE customer_risk_category
		WHEN 'High'
			THEN 1
		WHEN 'Medium'
			THEN 2
		ELSE 3
		END;


-- ================================================================
-- Q2. PEP / High-Risk Customer Transaction Activity
-- Purpose: Identify transactions conducted by customers requiring
--          enhanced monitoring (PEPs or High-Risk customers)
-- ================================================================

SELECT customer_id
	,account_id
	,full_name
	,pep_status
	,customer_risk_category
	,customer_country
	,customer_country_risk_level
	,clean_tx_date
	,ROUND(tx_amount_eur, 2) AS tx_amount_eur
	,tx_type
	,is_cash
	,tx_country
	,tx_country_risk_level
	,CASE 
		WHEN pep_status = 'Y'
			AND customer_risk_category = 'High'
			THEN 'PEP + High Risk'
		WHEN pep_status = 'Y'
			THEN 'PEP Activity'
		ELSE 'High-Risk Customer Activity'
		END AS alert_reason
FROM vw_customer_data
WHERE pep_status = 'Y'
	OR customer_risk_category = 'High'
ORDER BY customer_id
	,clean_tx_date;


-- ================================================================
-- Q3a. High-Value Transaction Activity Monitoring
-- Purpose: Identify single transactions exceeding 
--          the predefined high-value threshold (≥100,000 EUR)
-- ================================================================

SELECT customer_id
	,account_id
	,full_name
	,customer_risk_category
	,clean_tx_date
	,ROUND(tx_amount_eur, 2) AS tx_amount_eur
	,tx_type
	,is_cash
	,tx_country
	,tx_country_risk_level
	,'High-Value Tx (≥100k EUR)' AS alert_reason
FROM vw_customer_data
WHERE tx_amount_eur >= 100000
ORDER BY tx_amount_eur DESC;


-- ================================================================
-- Q3b. High Cumulative Transaction Activity Monitoring
-- Purpose: Identify accounts with cumulative transaction value
--          exceeding the predefined threshold (≥250,000 EUR)
-- ================================================================

WITH cumulative_amount
AS (
	SELECT account_id
		,customer_id
		,full_name
		,SUM(tx_amount_eur) AS total_tx_amount_eur
	FROM vw_customer_data
	GROUP BY account_id
		,customer_id
		,full_name
	)
SELECT account_id
	,customer_id
	,full_name
	,ROUND(total_tx_amount_eur, 2) AS total_tx_amount_eur
	,'High Cumulative Tx (≥250k EUR)' AS alert_reason
FROM cumulative_amount
WHERE total_tx_amount_eur >= 250000
ORDER BY total_tx_amount_eur DESC;


-- ================================================================
-- Q4. High-Risk Country Transaction Exposure
-- Purpose: Identify transactions involving jurisdictions
--          classified as high-risk based on country risk ratings
-- ================================================================

SELECT customer_id
	,account_id
	,full_name
	,customer_risk_category
	,clean_tx_date
	,ROUND(tx_amount_eur, 2) AS tx_amount_eur
	,tx_type
	,is_cash
	,tx_country
	,tx_country_risk_level
	,'High-Risk Country Tx' AS alert_reason
FROM vw_customer_data
WHERE tx_country_risk_level = 'High'
ORDER BY customer_id
	,clean_tx_date;


-- ================================================================
-- Q5. Cash Aggregation Detection
-- Purpose: Identify customers conducting multiple
--          sub-threshold cash deposits (<10,000 EUR)
--          indicating potential aggregation behaviour
-- ================================================================

WITH cash_aggregation
AS (
	SELECT customer_id
		,full_name
		,COUNT(*) AS no_of_cash_tx
		,SUM(tx_amount_eur) AS total_cash_tx_amount
	FROM vw_customer_data
	WHERE tx_type = 'Cash Deposit'
		AND tx_amount_eur < 10000
	GROUP BY customer_id
		,full_name
	)
SELECT customer_id
	,full_name
	,no_of_cash_tx
	,ROUND(total_cash_tx_amount, 2) AS total_cash_tx_amount
	,'Cash Aggregation' AS alert_reason
FROM cash_aggregation
WHERE no_of_cash_tx >= 3
	AND total_cash_tx_amount >= 10000
ORDER BY total_cash_tx_amount DESC;


-- ================================================================
-- Q6. Structuring Detection (7-Day Rolling Window)
-- Purpose: Identify customers conducting multiple
--          sub-threshold cash deposits (<10,000 EUR) 
--          within a 7-day rolling window,
--          indicating potential structuring behaviour
-- ================================================================

WITH cash_tx
AS (
	SELECT customer_id
		,clean_tx_date
		,tx_amount_eur
	FROM vw_customer_data
	WHERE tx_type = 'Cash Deposit'
		AND tx_amount_eur < 10000
	)
	,anchors
AS (
	SELECT DISTINCT customer_id
		,clean_tx_date
	FROM cash_tx
	)
	,rolling_structuring
AS (
	SELECT a.customer_id
		,a.clean_tx_date
		,COUNT(b.tx_amount_eur) AS no_of_cash_tx_in_7_days
		,SUM(b.tx_amount_eur) AS total_cash_tx_amount_in_7_days
	FROM anchors a
	JOIN cash_tx b ON a.customer_id = b.customer_id
		AND b.clean_tx_date BETWEEN DATE (
					a.clean_tx_date
					,'-7 days'
					)
			AND a.clean_tx_date
	GROUP BY a.customer_id
		,a.clean_tx_date
	)
SELECT customer_id
	,clean_tx_date
	,no_of_cash_tx_in_7_days
	,ROUND(total_cash_tx_amount_in_7_days, 2) AS total_cash_tx_amount_in_7_days
	,'Structuring (7D)' AS alert_reason
FROM rolling_structuring
WHERE no_of_cash_tx_in_7_days >= 3
	AND total_cash_tx_amount_in_7_days >= 10000
ORDER BY total_cash_tx_amount_in_7_days DESC;


-- ================================================================
-- Q7. Wire Transfer Velocity Detection (1-Day Rolling Window)
-- Purpose: Identify accounts conducting multiple
--          wire transfers with high cumulative value (≥50,000 EUR) 
--          within a 1-day rolling window,
--          indicating abnormal transaction velocity
-- ================================================================

WITH wire_velocity
AS (
	SELECT account_id
		,clean_tx_date
		,COUNT(*) AS no_of_wire_tx_in_1_day
		,SUM(tx_amount_eur) AS total_wire_tx_amount_in_1_day
	FROM vw_customer_data
	WHERE tx_type = 'Wire Transfer'
	GROUP BY account_id
		,clean_tx_date
	)
SELECT account_id
	,clean_tx_date
	,no_of_wire_tx_in_1_day
	,ROUND(total_wire_tx_amount_in_1_day, 2) AS total_wire_tx_amount_in_1_day
	,'Wire Velocity (1D)' AS alert_reason
FROM wire_velocity
WHERE no_of_wire_tx_in_1_day >= 3
	AND total_wire_tx_amount_in_1_day >= 50000
ORDER BY total_wire_tx_amount_in_1_day DESC;



/* ================================================================
   PERFORMANCE & IMPLEMENTATION NOTES (DOCUMENTATION)
   ================================================================ */

-- Recommended indexes for production-scale environments:

-- customers(customer_id)
-- transactions(customer_id)
-- transactions(account_id)
-- transactions(clean_tx_date)
-- country_risk(country)

-- These indexes would improve performance for aggregation,
-- rolling-window analysis, and transaction monitoring queries
-- operating on large datasets.

-- Index creation statements are intentionally omitted as
-- this project focuses on analytical detection logic rather
-- than physical database optimisation.


/* ================================================================
   End of Project 3: AML Transaction Monitoring Scenarios

   All views and monitoring queries above are designed as
   reusable analytical components for AML investigation and
   scenario prototyping.
   ================================================================ */
