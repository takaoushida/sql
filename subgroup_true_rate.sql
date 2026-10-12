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

with
select_tb as(
    select  
        created_at,
        stock_code,
        t1.up_flg,
        t2.down_flg,
        case
            when t1.subgroup_{flg}_rate is not null and t2.subgroup_{flg}_rate is null then t1.subgroup_{flg}_rate
            when t2.subgroup_{flg}_rate is not null and t1.subgroup_{flg}_rate is null then t2.subgroup_{flg}_rate
            when t1.subgroup_{flg}_rate is not null and t1.{flg}_flg = 1 then t1.subgroup_{flg}_rate
            when t2.subgroup_{flg}_rate is not null and t1.{flg}_flg = 0 then t2.subgroup_{flg}_rate
            else coalesce(t1.subgroup_{flg}_rate,t2.subgroup_{flg}_rate)
        end as subgroup_{flg}_rate,
        case
            when t1.rule is not null and t2.rule is null then t1.rule
            when t2.rule is not null and t1.rule is null then t2.rule
            when t1.rule is not null and t1.{flg}_flg = 1 then t1.rule
            when t2.rule is not null and t1.{flg}_flg = 0 then t2.role
            else coalesce(t1.rule,t2.rule)
        end as rule
    from    
        temp_folder.all_group_forecast_{flg}_high as t1
    left join
        temp_folder.all_group_forecast_{flg}_low as t2
        using(created_at,stock_code)        
),
aggre_tb as(
    select
        cast(subgroup_{flg}_rate * 100 as int64) as {flg}_rate_int,
        count(case when {flg}_flg = 1 then stock_code end) / count(stock_code) as subgroup_{flg}_rate
    from    
        select_tb
    where
        created_at between '2016-06-01' and '2025-03-31'
    group by 1
),
all_groups as(
    select * from feature_learning_prd.origin_all_group_up_high union all
    select * from feature_learning_prd.origin_all_group_up_low
)
select  
    distinct 
    t1.rule,
    t1.{flg}_rate as origin_{flg}_rate,
    t2.subgroup_up_rate
from
    all_groupos as t1
left join
    aggre_tb as t2
    on cast(t1.{flg}_rate * 100 as int64) =t2.{flg}_rate_int