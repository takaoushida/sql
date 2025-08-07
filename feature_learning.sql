create or replace table looker_datamart.feature_learning
partition by created_at
cluster by stock_code as(
with
stock_data as(
    select * from looker_datamart.stock_data_explanatory_valiable_add
    where created_at >=  '2020-01-01'
),
later_close_tb as(
    select
        *,
        max(created_at) over() as max_date
    from
        stock_data
),
latest_close_tb as(
    select
        *,
    from
        later_close_tb
    where
        created_at = max_date
)
select
    t1.*,
    t2.close as latest_close,
    t3.forcast_win_rate,
    t4.forcast_lose_rate
from
    stock_data  as t1
left join
    latest_close_tb as t2
    on t1.stock_code = t2.stock_code    
left join
    looker_datamart.feature_learning_win as t3
    on t1.stock_code = t3.stock_code and t1.created_at = t3.created_at
left join
    looker_datamart.feature_learning_lose as t4
    on t1.stock_code = t4.stock_code and t1.created_at = t4.created_at
)

