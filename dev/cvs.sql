USE ROLE MARKETING_CUSTOMER_ANALYTICS;
USE DATABASE customer_analytics;
USE SCHEMA sandbox;
USE WAREHOUSE ADW_HEAVY_ADHOC_WH;

-- set the financial periods that are required
SET (f_start, f_end) = (201903, 202002);

-- set the weights
set recency = 1;
set regularity = 2;
set spend = 3.5;
set tenure = 2;
set sow	= 0.75;
set touchpoints = 0.75;

-- from the financial periods get the start and end date
set (start_date, end_date) = (SELECT MIN(DATE_KEY)
                                , MAX(DATE_KEY)
                              FROM adw_prod.inc_pl.date_dim
                              WHERE fin_period_no BETWEEN $F_START AND $F_END);

-- set the number of days that need to have passed for a customer to be considered as lapsed
set days_between_purchases_cutoff = 84;

-- DEFINE THE CUSTOMER DOMAIN
-- create a table that contains all customers and flags active customers
CREATE
	OR replace TEMP TABLE cvs_all_customers AS
SELECT active_customers.*
	, customer_key
FROM
    (SELECT c.inferred_customer_id
    	, MAX(CASE WHEN t.date_key BETWEEN $start_date AND $end_date
    		  AND transaction_value > 0
    		  AND transaction_item_count > 0 THEN 1 ELSE 0 END) AS active_customer
    FROM adw_prod.inc_pl.customer_dim AS c
    LEFT JOIN adw_prod.inc_pl.transaction_dim AS t
    ON c.customer_key = t.customer_key
    GROUP BY 1) AS active_customers
LEFT JOIN adw_prod.inc_pl.customer_dim AS all_customers
ON active_customers.inferred_customer_id = all_customers.inferred_customer_id;

-- consider only active customers for scoring
-- inactive customers should automatically get a score of 0 assigned to them
CREATE
	OR replace TEMP TABLE cvs_active_customers AS
SELECT inferred_customer_id
	, customer_key
FROM cvs_all_customers
WHERE active_customer = 1;

-- SPEND
-- calculate total spend
CREATE
	OR replace TEMP TABLE cvs_spend AS
SELECT inferred_customer_id
	, IFNULL(SUM(transaction_value), 0) AS spend
FROM cvs_active_customers AS c
LEFT JOIN adw_prod.inc_pl.transaction_dim AS t
	ON c.customer_key = t.customer_key
WHERE date_key BETWEEN $start_date AND $end_date
GROUP BY 1;

-- RECENCY
-- calculate the days since the last purchase
CREATE
	OR replace TEMP TABLE cvs_recency AS
SELECT inferred_customer_id
	, DATEDIFF(d, MAX(t.date_key), $end_date) + 1 AS recency
FROM cvs_active_customers AS c
LEFT JOIN adw_prod.inc_pl.transaction_dim AS t
	ON c.customer_key = t.customer_key
WHERE t.date_key BETWEEN $start_date AND $end_date
GROUP BY 1;

-- REGULARITY
-- calculate shopping frequency
--- Part 1 - create a lead for the purchase date
CREATE
	OR replace TEMP TABLE cvs_regularity_part1 AS
SELECT inferred_customer_id
    , t.date_key
    , LEAD(t.date_key, 1) OVER (
        PARTITION BY inferred_customer_id ORDER BY t.date_key
        ) AS lead_date_key
FROM cvs_active_customers AS c
LEFT JOIN adw_prod.inc_pl.transaction_dim AS t
    ON c.customer_key = t.customer_key
WHERE date_key BETWEEN $start_date AND $end_date
-- following what was done previously
AND t.transaction_value > 0
GROUP BY 1, 2;

--- Part 2 - substract the current purchase date from the next purchase date to get the number of days between purchases
--- calculate percentiles based on days between purchases
CREATE
	OR replace TEMP TABLE cvs_regularity_part2 AS
SELECT inferred_customer_id
	, date_key AS purchase_date
	, lead_date_key AS next_purchase_date
	, DATEDIFF(d, date_key, lead_date_key) AS days_between_purchases
	, PERCENTILE_DISC(0.25) within
GROUP (
		ORDER BY DATEDIFF(d, date_key, lead_date_key)
		) OVER (PARTITION BY inferred_customer_id) AS reg_q1
	, PERCENTILE_DISC(0.75) within
GROUP (
		ORDER BY DATEDIFF(d, date_key, lead_date_key)
		) OVER (PARTITION BY inferred_customer_id) AS reg_q3
FROM (
      SELECT a.*
      FROM cvs_regularity_part1 AS a
      LEFT JOIN (
            SELECT inferred_customer_id
                , CASE WHEN COUNT(date_key) > 2 THEN 1 ELSE 0 END AS transaction_days_count
            FROM cvs_regularity_part1
            WHERE lead_date_key IS NOT NULL
            GROUP BY 1) AS b
      ON a.inferred_customer_id = b.inferred_customer_id
      WHERE transaction_days_count = 1
) AS sub;

--- Part 3
CREATE
	OR replace TEMP TABLE cvs_regularity AS
SELECT inferred_customer_id
	, IFNULL(AVG(days_between_purchases),0) AS regularity
	, IFNULL(LOG(10, (AVG(days_between_purchases) + 1)),0) AS log_regularity
FROM cvs_regularity_part2
WHERE days_between_purchases <= (reg_q3 + (reg_q3 - reg_q1) * 1.5)
	AND days_between_purchases IS NOT NULL
GROUP BY 1;

-- TENURE
CREATE OR replace TEMP TABLE cvs_tenure_regularity AS
SELECT inferred_customer_id
    , date_key AS purchase_date
    , transaction_key
    , lead_date_key AS next_purchase_date
    , DATEDIFF(d, date_key, lead_date_key) AS days_between_purchases
FROM(
  SELECT inferred_customer_id
      , date_key
      , transaction_key
      , LEAD(date_key, 1) OVER(PARTITION BY inferred_customer_id order by date_key) AS lead_date_key
  FROM cvs_active_customers AS c
  INNER JOIN adw_prod.inc_pl.transaction_dim AS t
  ON c.customer_key = t.customer_key
  WHERE date_key <= $end_date
  GROUP BY 1,2,3
  ORDER BY 1,2,3) AS sub;

CREATE OR replace TEMP TABLE cvs_tenure_run_info AS
SELECT inferred_customer_id
    , transaction_key
    , days_between_purchases
    , LAG(next_purchase_date, 1) OVER(PARTITION BY inferred_customer_id ORDER BY next_purchase_date) AS run_start_date
    , purchase_date AS run_end_date
FROM cvs_tenure_regularity
WHERE days_between_purchases > $days_between_purchases_cutoff OR days_between_purchases IS NULL;

CREATE OR replace TEMP TABLE cvs_tenure_purchase_info AS
SELECT inferred_customer_id
    , MIN(date_key) AS first_purchase_date
    , MAX(date_key) AS last_purchase_date
FROM cvs_active_customers AS c
INNER JOIN adw_prod.inc_pl.transaction_dim AS t
ON c.customer_key = t.customer_key
AND date_key <= $end_date
GROUP BY 1;

CREATE OR replace TEMP TABLE cvs_run_start_end AS
SELECT r.inferred_customer_id
    , days_between_purchases
    , IFNULL(run_start_date, first_purchase_date) AS run_start
    , CASE WHEN days_between_purchases IS NULL
        AND IFNULL(run_start_date, first_purchase_date) IS NOT NULL
        THEN IFF(DATEDIFF(d, last_purchase_date, $end_date) > $days_between_purchases_cutoff, last_purchase_date, $end_date)
        ELSE run_end_date END AS run_end
    , IFF(DATEDIFF(d, last_purchase_date, $end_date) > $days_between_purchases_cutoff, 1, 0) AS lapsed
    , ROW_NUMBER() over (partition by r.inferred_customer_id ORDER BY IFNULL(run_start_date, first_purchase_date)) AS run_number
FROM cvs_tenure_run_info AS r
LEFT JOIN cvs_tenure_purchase_info AS p
ON r.inferred_customer_id = p.inferred_customer_id;

CREATE OR replace TEMP TABLE cvs_tenure_combined AS
SELECT c.inferred_customer_id
    ,date_key
    ,run_number
    ,run_start
    ,run_end
    ,lapsed
FROM cvs_active_customers AS c
INNER JOIN adw_prod.inc_pl.transaction_dim AS t
ON c.customer_key = t.customer_key
LEFT JOIN cvs_run_start_end AS r
ON c.inferred_customer_id = r.inferred_customer_id
AND run_start <= date_key
AND run_end >= date_key
AND date_key <= $end_date
ORDER BY 1,2;

CREATE OR replace TEMP TABLE cvs_tenure AS
SELECT inferred_customer_id
    , MAX(run_number) AS number_of_runs
    , MAX(lapsed) AS lapsed
    , IFF(MAX(lapsed) = 0, DATEDIFF(d, MAX(run_start), MAX(run_end)), 0) AS tenure --days_in_latest_run
FROM cvs_tenure_combined
GROUP BY 1;

-- SOW SHARE OF WALLET
CREATE
	OR replace TEMP TABLE cvs_sow AS
SELECT inferred_customer_id
	, AVG(sow) AS avg_sow
FROM (
	SELECT inferred_customer_id
		, fin_period_no
		, sow
		, PERCENTILE_CONT(0.25) within
	GROUP (
			ORDER BY sow
			) OVER (PARTITION BY inferred_customer_id) AS sow_q1
		, PERCENTILE_CONT(0.75) within
	GROUP (
			ORDER BY sow
			) OVER (PARTITION BY inferred_customer_id) AS sow_q3
	FROM cvs_active_customers AS c
	LEFT JOIN adw_prod.adw_pl.loyalty_segments AS l
		ON l.enterprise_customer_num = c.inferred_customer_id
	LEFT JOIN adw_prod.inc_pl.date_dim AS d
		ON l.week_no = d.week_no
	WHERE d.fin_period_no BETWEEN $F_START AND $F_END) AS sub
WHERE sow <= (sow_q3 + (sow_q3 - sow_q1) * 1.5)
	AND sow >= (sow_q1 - (sow_q3 - sow_q1) * 1.5)
GROUP BY 1;

-- TOUCHPOINTS
--- Part 1: Store Touchpoints
CREATE
	OR replace TEMP TABLE cvs_store_touchpoints AS
SELECT inferred_customer_id
	, MAX(CASE WHEN UPPER(store_format) = 'CONVENIENCE STORES' THEN 1 ELSE 0 END) AS convenience_store_flag
	, MAX(CASE WHEN UPPER(store_format) = 'PETROL STATIONS' THEN 1 ELSE 0 END) AS fuel_station_flag
	, MAX(CASE WHEN UPPER(store_format) = 'RESTAURANTS' THEN 1 ELSE 0 END) AS restaurant_falg
	, MAX(CASE WHEN UPPER(store_format) = 'SUPERSTORES' THEN 1 ELSE 0 END) AS superstore_flag
    , MAX(CASE WHEN UPPER(store_format) = 'UNKNOWN' THEN 1 ELSE 0 END) AS unknown_flag
	, MAX(CASE WHEN party_account_type_code = 2 THEN 1 ELSE 0 END) AS online_flag
FROM cvs_active_customers AS c
LEFT JOIN adw_prod.inc_pl.transaction_dim AS t
	ON c.customer_key = t.customer_key
LEFT JOIN adw_prod.inc_pl.location_dim AS l
	ON t.location_key = l.location_key
LEFT JOIN adw_prod.inc_pl.date_dim AS d
	ON t.date_key = d.date_key
WHERE d.fin_period_no BETWEEN $F_START AND $F_END
GROUP BY 1;

--- Part 2: Brand Touchpoints
CREATE
	OR replace TEMP TABLE cvs_brand_touchpoints AS
SELECT inferred_customer_id
	, MAX(CASE WHEN UPPER(sainsburys_bank_flag) = 'Y' THEN 1 ELSE 0 END) AS bank_flag
	, MAX(CASE WHEN UPPER(js_nectar_flag) = 'Y' THEN 1 ELSE 0 END) AS nectar_flag
	, MAX(CASE WHEN UPPER(js_tu_flag) = 'Y' THEN 1 ELSE 0 END) AS tu_flag
FROM cvs_active_customers AS c
LEFT JOIN adw_prod.adw_pl.dim_group_customer AS t
	ON t.enterprise_customer_id = c.inferred_customer_id
GROUP BY 1;

--- Part 3: GM Instore and Online Touchpoints
CREATE
	OR replace TEMP TABLE cvs_gm AS
SELECT c.inferred_customer_id
	, MAX(CASE WHEN director_no IN (1230, 2402)
				AND party_account_type_code = 4 THEN 1 ELSE 0 END) AS gm_instore
	, MAX(CASE WHEN director_no IN (1230, 2402)
				AND party_account_type_code = 2 THEN 1 ELSE 0 END) AS gm_online
	, MAX(CASE WHEN director_no IN (1230, 2402) THEN 1 ELSE 0 END) AS gm
FROM cvs_active_customers AS c
LEFT JOIN adw_prod.inc_pl.item_fact AS f
	ON c.customer_key = f.customer_key
LEFT JOIN adw_prod.inc_pl.product_dim AS p
	ON f.product_key = p.product_key
LEFT JOIN adw_prod.inc_pl.date_dim AS d
on f.date_key = d.date_key
WHERE d.fin_period_no BETWEEN $F_START AND $F_END
GROUP BY 1;

--- Part 4 - In the future, we might want to include Argos customers and use a weighted sum
CREATE
	OR replace TEMP TABLE cvs_touchpoints AS
SELECT c.inferred_customer_id
	, (s.convenience_store_flag
       + s.fuel_station_flag
       + s.restaurant_falg
       + s.superstore_flag
       + s.online_flag
       + b.bank_flag
       + b.nectar_flag
       + b.tu_flag
       + g.gm_instore
       + g.gm_online) AS touchpoints
FROM (
      SELECT distinct inferred_customer_id
      FROM cvs_active_customers) AS c
LEFT JOIN cvs_store_touchpoints AS s
	ON c.inferred_customer_id = s.inferred_customer_id
LEFT JOIN cvs_brand_touchpoints AS b
	ON c.inferred_customer_id = b.inferred_customer_id
LEFT JOIN cvs_gm AS g
	ON c.inferred_customer_id = g.inferred_customer_id;

-- COMBINE ALL THE MAIN TABLES
CREATE
	OR replace TEMP TABLE cvs_all_features AS
SELECT c.inferred_customer_id
    , active_customer
    , spend
    , recency
	, regularity
	, log_regularity
	, tenure
	, avg_sow
	, IFF(touchpoints = 0, 1, touchpoints) AS touchpoints
FROM (
      SELECT inferred_customer_id
          , MAX(active_customer) AS active_customer
      FROM cvs_all_customers
      GROUP BY 1) AS c
LEFT JOIN cvs_spend AS s
	ON c.inferred_customer_id = s.inferred_customer_id
LEFT JOIN cvs_recency AS r
	ON c.inferred_customer_id = r.inferred_customer_id
LEFT JOIN cvs_regularity AS f
	ON c.inferred_customer_id = f.inferred_customer_id
LEFT JOIN cvs_tenure AS t
	ON c.inferred_customer_id = t.inferred_customer_id
LEFT JOIN cvs_sow AS sow
	ON c.inferred_customer_id = sow.inferred_customer_id
LEFT JOIN cvs_touchpoints AS tp
	ON c.inferred_customer_id = tp.inferred_customer_id;


-- SPEND SCORING
-- Any total spend that is negative should get the lowest score
CREATE
	OR replace TEMP TABLE cvs_log_spend AS
SELECT inferred_customer_id
    , CASE WHEN spend >= 0 THEN LOG(10, spend + 1) ELSE LOG(10, 1) END AS log_spend
from cvs_all_features
where active_customer = 1;

CREATE
	OR replace TEMP TABLE cvs_zscore_log_spend AS
SELECT inferred_customer_id
    , (log_spend - avg_log_spend)/std_log_spend AS zscore_log_spend
FROM cvs_log_spend
JOIN (SELECT AVG(log_spend) AS avg_log_spend
        , STDDEV(log_spend) AS std_log_spend
      FROM cvs_log_spend);

CREATE
	OR replace TEMP TABLE cvs_spend_score AS
SELECT inferred_customer_id
    , (zscore_log_spend + min_zscore_log_spend) / (max_zscore_log_spend + min_zscore_log_spend) * 10 AS spend_score
FROM cvs_zscore_log_spend
JOIN (SELECT ABS(MIN(zscore_log_spend)) AS min_zscore_log_spend
        , MAX(zscore_log_spend) AS max_zscore_log_spend
      FROM cvs_zscore_log_spend);

-- REGULARITY SCORING
-- Missing average days between purchases will be assigned the lowest score
-- might bw better to set missing values to the MAX(log_regulrity)
CREATE
	OR replace TEMP TABLE cvs_zscore_log_regularity AS
SELECT inferred_customer_id
    , (log_regularity - avg_log_regularity)/std_log_regularity AS zscore_log_regularity
FROM cvs_all_features
JOIN (SELECT AVG(log_regularity) AS avg_log_regularity
        , STDDEV(log_regularity) AS std_log_regularity
      FROM cvs_all_features)
WHERE active_customer = 1;

CREATE
	OR replace TEMP TABLE cvs_regularity_score AS
SELECT inferred_customer_id
    , IFNULL(ABS(zscore_log_regularity - max_zscore_log_regularity)/comb_zscore_log_regularity * 10, 0) as regularity_score
FROM cvs_zscore_log_regularity
JOIN (SELECT MAX(zscore_log_regularity) AS max_zscore_log_regularity
        , ABS(MIN(zscore_log_regularity)) + MAX(zscore_log_regularity) as comb_zscore_log_regularity
      FROM cvs_zscore_log_regularity);

-- TOUCHPOINTS SCORING
-- decided not to add .00001 to the zscore
CREATE
	OR replace TEMP TABLE cvs_log_touchpoints AS
SELECT inferred_customer_id
    , LOG(10, touchpoints) AS log_touchpoints
FROM cvs_all_features
WHERE active_customer = 1;

CREATE
	OR replace TEMP TABLE cvs_zscore_log_touchpoints AS
SELECT inferred_customer_id
    , (log_touchpoints - avg_log_touchpoints)/std_log_touchpoints AS zscore_log_touchpoints
FROM cvs_log_touchpoints
JOIN (SELECT AVG(log_touchpoints) AS avg_log_touchpoints
        , STDDEV(log_touchpoints) AS std_log_touchpoints
      FROM cvs_log_touchpoints);

CREATE
	OR replace TEMP TABLE cvs_touchpoints_score AS
SELECT inferred_customer_id
    , IFNULL((zscore_log_touchpoints + min_zscore_log_touchpoints)
                 / (max_zscore_log_touchpoints + min_zscore_log_touchpoints) * 10,0) AS touchpoints_score
FROM cvs_zscore_log_touchpoints
JOIN (SELECT ABS(MIN(zscore_log_touchpoints)) AS min_zscore_log_touchpoints
        , MAX(zscore_log_touchpoints) AS max_zscore_log_touchpoints
      FROM cvs_zscore_log_touchpoints);

-- TENURE SCORING

CREATE
	OR replace TEMP TABLE cvs_log_tenure AS
SELECT inferred_customer_id
    , CASE WHEN tenure >= 0 THEN LOG(10, tenure + 1) ELSE LOG(10, 1) END AS log_tenure
from cvs_all_features
where active_customer = 1;

CREATE
	OR replace TEMP TABLE cvs_zscore_log_tenure AS
SELECT inferred_customer_id
    , (log_tenure - avg_log_tenure)/std_log_tenure AS zscore_log_tenure
FROM cvs_log_tenure
JOIN (SELECT AVG(log_tenure) AS avg_log_tenure
        , STDDEV(log_tenure) AS std_log_tenure
      FROM cvs_log_tenure);

CREATE
	OR replace TEMP TABLE cvs_tenure_score AS
SELECT inferred_customer_id
    , (zscore_log_tenure + min_zscore_log_tenure) / (max_zscore_log_tenure + min_zscore_log_tenure) * 10 AS tenure_score
FROM cvs_zscore_log_tenure
JOIN (SELECT ABS(MIN(zscore_log_tenure)) AS min_zscore_log_tenure
        , MAX(zscore_log_tenure) AS max_zscore_log_tenure
      FROM cvs_zscore_log_tenure);

-- SHARE OF WALLET SCORING
CREATE
	OR replace TEMP TABLE cvs_sow_score AS
SELECT inferred_customer_id
    , IFNULL(avg_sow*10, 0) AS sow_score
FROM cvs_all_features
where active_customer = 1;

-- RECENCY SCORING
CREATE
	OR replace TEMP TABLE cvs_recency_cdf AS
SELECT inferred_customer_id
    , recency
    , CASE WHEN recency < 0 THEN 0 ELSE 1 - EXP(-recency/25) END AS cdf_recency
FROM cvs_all_features
WHERE active_customer = 1;

CREATE
	OR replace TEMP TABLE cvs_recency_score AS
SELECT inferred_customer_id
    , ((cdf_recency * -1) + 1) / max_cdf * 10 AS recency_score
FROM cvs_recency_cdf
JOIN (
  SELECT MAX(CASE WHEN recency = 1 THEN 1 - cdf_recency ELSE 0 END) AS max_cdf
  FROM cvs_recency_cdf);

-- FINAL SCORE
CREATE
	OR replace TABLE cvs_score_new AS
SELECT a.inferred_customer_id
    , CASE WHEN active_customer = 1 THEN
      (spend_score * $spend
      + regularity_score * $regularity
      + touchpoints_score * $touchpoints
      + tenure_score * $tenure
      + sow_score * $sow
      + recency_score * $recency) ELSE 0 END AS score
   , spend
   , regularity
   , touchpoints
   , tenure
   , avg_sow
   , recency
FROM cvs_all_features AS a
LEFT JOIN cvs_spend_score AS b
    ON a.inferred_customer_id = b.inferred_customer_id
LEFT JOIN cvs_regularity_score AS c
    ON a.inferred_customer_id = c.inferred_customer_id
LEFT JOIN cvs_touchpoints_score AS d
    ON a.inferred_customer_id = d.inferred_customer_id
LEFT JOIN cvs_tenure_score AS e
    ON a.inferred_customer_id = e.inferred_customer_id
LEFT JOIN cvs_sow_score AS f
    ON a.inferred_customer_id = f.inferred_customer_id
LEFT JOIN cvs_recency_score AS g
    ON a.inferred_customer_id = g.inferred_customer_id;