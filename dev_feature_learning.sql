create or replace table feature_learning_dev.feature_learning
partition by created_at
cluster by stock_code as(
with
stock_data as(
    select * from feature_learning_dev.stock_data_explanatory_valiable_add_20251216_120day_restore
    where created_at between '2016-06-01' and '2025-03-31'
)
select
    t1.*,
    t2.forcast_up_rate,
    t3.forcast_down_rate,
    t4.forcast_win_rate,
    t5.forcast_lose_rate
from
    stock_data  as t1
left join
    feature_learning_dev.dev_feature_learning_up as t2
    on t1.stock_code = t2.stock_code and t1.created_at = t2.created_at
left join
    feature_learning_dev.dev_feature_learning_down as t3
    on t1.stock_code = t3.stock_code and t1.created_at = t3.created_at
left join
    feature_learning_dev.dev_feature_learning_win as t4
    on t1.stock_code = t4.stock_code and t1.created_at = t4.created_at
left join
    feature_learning_dev.dev_feature_learning_lose as t5
    on t1.stock_code = t5.stock_code and t1.created_at = t5.created_at
)

