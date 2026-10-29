create or replace table looker_datamart.feature_learning
partition by created_at
cluster by stock_code as(
with
stock_data as(
    select 
        created_at,
        stock_code,
        close,
        volume,
        quarter_net_income_rate,
        roa,
        roe,
        pbr,
        per,
        stock_reward,
        reward_rate,
        UP_flg,
        down_flg,
        win_flg,
        lose_flg,
        weather,
        supervision_reason,
        ipo_flg,
        irregular_flg,
        stock_split,
        buyback_flg,
        lightgbm_up_rate,
        lightgbm_down_rate,
    from 
        temp_folder.pre_feature_learning_up as t1
    left join
        temp_folder.pre_feature_learning_down as t2
        using(created_at,stock_code)
    where 
        created_at >= '2025-04-01'
),
joint_tb as(
    select
        t1.*,
        t2.forecast_up_rate,
        t3.forecast_down_rate,
        row_number() over(partition by t1.stock_code order by t1.created_at) as row_number
    from
        stock_data  as t1
    left join
        temp_folder.feature_learning_up  as t2
        using(created_at,stock_code)
    left join
        temp_folder.feature_learning_down as t3
        using(created_at,stock_code)
),
suggest_add as(
    select
        *,
        case
            when forcast_up_rate >= 0.9 and subgroup_up_rate >= 0.9 and forcast_down_rate < 0.2 and pbr >= 0 and irregular_flg is null and supervision_reason is null then '90%以上'
            when forcast_up_rate >= 0.8 and subgroup_up_rate >= 0.8 and forcast_down_rate < 0.1 and pbr >= 0 and irregular_flg is null and supervision_reason is null then '80%以上'
            when forcast_up_rate >= 0.7 and subgroup_up_rate >= 0.7 and forcast_down_rate < 0.1 and pbr >= 0 and irregular_flg is null and supervision_reason is null and ifnull(stock_split,0) not in(2,3) and buyback_flg is null and reward_rate >= 0.04 then '高配当'   
            when forcast_up_rate >= 0.95 and forcast_down_rate < 0.1 and pbr >= 0 and irregular_flg is null and supervision_reason is null and ifnull(stock_split,0) not in(2,3) and buyback_flg is null and reward_rate >= 0.04 then '高配当'   
            when forcast_up_rate >= 0.9 and subgroup_up_rate >= 0.7 and forcast_down_rate < 0.1 and pbr >= 0 and irregular_flg is null and supervision_reason is null and ifnull(stock_split,0) not in(2,3) and buyback_flg is null and weather in(4,5) then '優良企業'
            when forcast_up_rate >= 0.9 and subgroup_up_rate >= 0.7 and forcast_down_rate < 0.1  and pbr >= 0 and irregular_flg is null and supervision_reason is null and ifnull(stock_split,0) not in(2,3) and buyback_flg is null then '確率のみ'
            when forcast_up_rate >= 0.9  and forcast_down_rate < 0.1 and pbr >= 0 and irregular_flg is null and supervision_reason is null and ifnull(stock_split,0) not in(2,3) and buyback_flg is null and reward_rate >= 0.04 then '高配当(微)'   
            when forcast_up_rate >= 0.9  and forcast_down_rate < 0.1 and pbr >= 0 and irregular_flg is null and supervision_reason is null and ifnull(stock_split,0) not in(2,3) and buyback_flg is null and weather in(4,5) then '優良企業(微)'
        end as suggest_type,
        case
            when forcast_up_rate >= 0.9 and subgroup_up_rate >= 0.9 and forcast_down_rate < 0.2 and pbr >= 0 and irregular_flg is null and supervision_reason is null then 1
            when forcast_up_rate >= 0.8 and subgroup_up_rate >= 0.8 and forcast_down_rate < 0.1 and pbr >= 0 and irregular_flg is null and supervision_reason is null then 2
            when forcast_up_rate >= 0.7 and subgroup_up_rate >= 0.7 and forcast_down_rate < 0.1 and pbr >= 0 and irregular_flg is null and supervision_reason is null and ifnull(stock_split,0) not in(2,3) and buyback_flg is null and reward_rate >= 0.04 then 3   
            when forcast_up_rate >= 0.95 and forcast_down_rate < 0.1 and pbr >= 0 and irregular_flg is null and supervision_reason is null and ifnull(stock_split,0) not in(2,3) and buyback_flg is null and reward_rate >= 0.04 then 3
            when forcast_up_rate >= 0.9 and subgroup_up_rate >= 0.7 and forcast_down_rate < 0.1 and pbr >= 0 and irregular_flg is null and supervision_reason is null and ifnull(stock_split,0) not in(2,3) and buyback_flg is null and weather in(4,5) then 4
            when forcast_up_rate >= 0.9 and subgroup_up_rate >= 0.7 and forcast_down_rate < 0.1  and pbr >= 0 and irregular_flg is null and supervision_reason is null and ifnull(stock_split,0) not in(2,3) and buyback_flg is null then 5
            when forcast_up_rate >= 0.9  and forcast_down_rate < 0.1 and pbr >= 0 and irregular_flg is null and supervision_reason is null and ifnull(stock_split,0) not in(2,3) and buyback_flg is null and reward_rate >= 0.04 then 6
            when forcast_up_rate >= 0.9  and forcast_down_rate < 0.1 and pbr >= 0 and irregular_flg is null and supervision_reason is null and ifnull(stock_split,0) not in(2,3) and buyback_flg is null and weather in(4,5) then 7
        end as suggest_sort, 
    from
        joint_tb
),
lag_add as(
    select
        *,
        lag(row_number,1) over(partition by stock_code,suggest_type order by created_at) as last_row_number
    from
        suggest_add
)
select
    *,
    case when (last_row_number is null or row_number - last_row_number >= 3) and suggest_type is not null then 1 end as dual_num
from
    lag_add
)






