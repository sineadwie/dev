USE ROLE MARKETING_CUSTOMER_ANALYTICS;
USE DATABASE customer_analytics;
USE SCHEMA sandbox;
USE WAREHOUSE ADW_HEAVY_ADHOC_WH;

-- set the financial periods that are required
SET (f_start, f_end) = (201903, 202002);

-- from the financial periods get the start and end date
set (start_date, end_date) = (SELECT MIN(DATE_KEY)
                                , MAX(DATE_KEY)
                              FROM adw_prod.inc_pl.date_dim
                              WHERE fin_period_no BETWEEN $F_START AND $F_END);

CREATE
	OR replace TEMP TABLE cvs_all_customers AS
SELECT active_customers.*
	, customer_key
FROM
    (SELECT c.inferred_customer_id
    	, MAX(CASE WHEN t.date_key BETWEEN $start_date AND $end_date
    		  AND transaction_value > 0
    		  AND transaction_item_count > 0 THEN 1 ELSE 0 END) AS active_customer
//fix to be more similar to old model
//        , 1 AS active_customer
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


create or replace table cvs_tenure_regularity as
select inferred_customer_id
    , date_key as purchase_date
    , transaction_key
    , lead_date_key as next_purchase_date
    , datediff(d, date_key, lead_date_key) as days_between_purchases
from(
  select inferred_customer_id
      , date_key
      , transaction_key
      , lead(date_key, 1) over(partition by inferred_customer_id order by date_key) as lead_date_key
  from cvs_active_customers as c
  inner join adw_prod.inc_pl.transaction_dim as t
  on c.customer_key = t.customer_key
  where date_key <= $end_date
  group by 1,2,3
  order by 1,2,3) as sub;

create or replace temp table cvs_tenure_run_info as
select inferred_customer_id
    , transaction_key
    , days_between_purchases
    , lag(next_purchase_date, 1) over(partition by inferred_customer_id order by next_purchase_date) as run_start_date
    , purchase_date as run_end_date
from cvs_tenure_regularity
where days_between_purchases > 84 or days_between_purchases is null;

create or replace temp table cvs_tenure_purchase_info as
select inferred_customer_id
    , min(date_key) as first_purchase_date
    , max(date_key) as last_purchase_date
from cvs_active_customers as c
inner join adw_prod.inc_pl.transaction_dim as t
on c.customer_key = t.customer_key
and date_key <= $end_date
group by 1;

create or replace temp table cvs_run_start_end as
select r.inferred_customer_id
    , days_between_purchases
    , ifnull(run_start_date, first_purchase_date) as run_start
    , case when days_between_purchases is null
        and ifnull(run_start_date, first_purchase_date) is not null
        then iff(datediff(d, last_purchase_date, $end_date) > 84, last_purchase_date, $end_date)
        else run_end_date end as run_end
    , iff(datediff(d, last_purchase_date, $end_date) > 84, 1, 0) as lapsed
    , row_number() over (partition by r.inferred_customer_id order by ifnull(run_start_date, first_purchase_date)) as run_number
from cvs_tenure_run_info as r
left join cvs_tenure_purchase_info as p
on r.inferred_customer_id = p.inferred_customer_id;

create or replace temp table cvs_tenure_combined as
select c.inferred_customer_id
    ,date_key
    ,run_number
    ,run_start
    ,run_end
    ,lapsed
from cvs_active_customers as c
inner join adw_prod.inc_pl.transaction_dim as t
on c.customer_key = t.customer_key
left join cvs_run_start_end as r
on c.inferred_customer_id = r.inferred_customer_id
and run_start <= date_key
and run_end >= date_key
and date_key <= $end_date
order by 1,2;

create or replace temp table cvs_tenure_combined as
select inferred_customer_id
    , max(run_number) as number_of_runs
    , max(lapsed) as lapsed
    , iff(max(lapsed) = 0, datediff(d, max(run_start), max(run_end)), 0) as days_in_latest_run
from cvs_tenure_combined
group by 1;

select * from cvs_tenure_combined
order by 1,2