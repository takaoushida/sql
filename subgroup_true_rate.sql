with
true_percent as(
    select
        cast(subgroup_{flg}_rate * 100 as int64) as forecast_rate,
        count(case when {flg}_flg = 1 then stock_code end) / count(stock_code) as subgroup_{flg}_rate,
    from
        temp_folder.all_group_forecast_{flg}_{stage_name}
    where
        created_at between '2016-06-01' and '2025-03-31'
    group by 1
)
select
    distinct
    t1.rule,
    t1.{flg}_rate as origin_{flg}_rate,
    t2.subgroup_{flg}_rate
from
    feature_learning_prd.origin_all_group_{flg}_{stage_name} as t1
left join   
    true_percent as t2
    on cast(t1.{flg}_rate * 100 as int64) = t2.forecast_rate
