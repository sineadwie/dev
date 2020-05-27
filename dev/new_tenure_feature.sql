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

create or replace table regularity_temp_sw as
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
  from active_customers_sw as c
  inner join adw_prod.inc_pl.transaction_dim as t
  on c.customer_key = t.customer_key
  where date_key <= $end_date
  group by 1,2,3
  order by 1,2,3) as sub;

select * from regularity_temp_sw
order by purchase_date;

create or replace temp table run_info_temp_sw as
select inferred_customer_id
    , transaction_key
    , days_between_purchases
    , lag(next_purchase_date, 1) over(partition by inferred_customer_id order by next_purchase_date) as run_start_date
    , purchase_date as run_end_date
from regularity_temp_sw
where days_between_purchases > 84 or days_between_purchases is null;

select * from run_info_temp_sw
order by run_start_date;

create or replace temp table purchase_info_temp_sw as
select inferred_customer_id
    , min(date_key) as first_purchase_date
    , max(date_key) as last_purchase_date
from active_customers_sw as c
inner join adw_prod.inc_pl.transaction_dim as t
on c.customer_key = t.customer_key
and date_key <= $end_date
group by 1;

select * from purchase_info_temp_sw
order by first_purchase_date;

create or replace temp table run_start_end_temp_sw as
select r.inferred_customer_id
    , days_between_purchases
    , ifnull(run_start_date, first_purchase_date) as run_start
    , case when days_between_purchases is null
        and ifnull(run_start_date, first_purchase_date) is not null
        then iff(datediff(d, last_purchase_date, $end_date) > 84/2, last_purchase_date, $end_date)
        else run_end_date end
        as run_end
    , row_number() over (partition by r.inferred_customer_id order by ifnull(run_start_date, first_purchase_date)) as run_number
from run_info_temp_sw as r
left join purchase_info_temp_sw as p
on r.inferred_customer_id = p.inferred_customer_id;

select * from run_start_end_temp_sw
order by run_number;

create or replace temp table run_final_sw as
select c.inferred_customer_id
    ,date_key
    ,run_number
    ,run_start
    ,run_end
from active_customers_sw as c
inner join adw_prod.inc_pl.transaction_dim as t
on c.customer_key = t.customer_key
left join run_start_end_temp_sw as r
on c.inferred_customer_id = r.inferred_customer_id
and run_start <= date_key
and run_end >= date_key
and date_key <= $end_date
order by 1,2;

select inferred_customer_id
    , max(run_number)
    , datediff(d, max(run_start), max(run_end)) as days_in_latest_run
from run_final_sw
group by 1;