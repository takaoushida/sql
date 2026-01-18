create or replace table looker_datamart.feature_learning
partition by created_at
cluster by stock_code as(
with
stock_data as(
    select * from looker_datamart.stock_data_explanatory_valiable_add
    where created_at >= '2016-06-01' 
),
joint_tb as(
    select
        t1.*,
        t2.forcast_up_rate,
        t3.forcast_down_rate,
        t4.forcast_win_rate,
        t5.forcast_lose_rate,
        min(t1.created_at) over(partition by t1.stock_code) as min_dt
    from
        stock_data  as t1
    left join
        temp_folder.feature_learning_up as t2
        on t1.stock_code = t2.stock_code and t1.created_at = t2.created_at
    left join
        temp_folder.feature_learning_down as t3
        on t1.stock_code = t3.stock_code and t1.created_at = t3.created_at
    left join
        temp_folder.feature_learning_win as t4
        on t1.stock_code = t4.stock_code and t1.created_at = t4.created_at
    left join
        temp_folder.feature_learning_lose as t5
        on t1.stock_code = t5.stock_code and t1.created_at = t5.created_at
),
suggest_add as(
    select
        *,
        case 
            when 
                rsi <90 and volume_ratio >= 30 and roc >= -7.5 and roc < 2.5
                and short_envelope >= 0.95 and envelope >= 0.95
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.025
                and forcast_win_rate >= 0.9 and forcast_lose_rate < 0.025
                and reward_rate >= 0.04
            then '高配当'
            when
                rsi > 0 and psychological >= 20 and roc >= -7.5 and roc < 2.5
                and short_envelope >= 0.95 and envelope >= 0.95 and long_envelope >= 0.95
                and top_relative_rate >= 0.4 and day60_top_relative_rate >= 0.8
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.025
                and forcast_win_rate >= 0.9 and forcast_lose_rate < 0.05
                and weather in(4,5)
            then '優良銘柄'
            when
                volume_ratio >= 30 and psychological < 60
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.025
                and forcast_win_rate >= 0.9 and forcast_lose_rate < 0.025
                and stock_split in(2,3)
            then '株式分割'
            when
                rsi < 70 and volume_ratio >= 60 and volume_ratio <220 and psychological < 60
                and roc >= -7.5 and roc < 5 and rci < 75
                and short_envelope >= 0.95 and envelope >= 0.95
                and bottom_relative_rate < 1.1 and day60_bottom_relative_rate < 1.1
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.2
                and forcast_win_rate >= 0.95 and forcast_lose_rate < 0.025
                and volatility <= 4 
            then '安値圏'
            when
                rsi >= 20 and volume_ratio >= 30 and volume_ratio < 250 and psychological >= 20 and psychological < 80
                and roc >= -7.5 and roc < 5 
                and short_envelope >= 0.95 and short_envelope < 1.01 and envelope >= 0.95 and envelope < 1.025
                and bottom_relative_rate is not null
                and forcast_up_rate >= 0.95 and forcast_down_rate < 0.025
                and forcast_win_rate >= 0.95 and forcast_lose_rate < 0.025
                and ifnull(pbr,0) < 10 and volatility <= 4 
                and buyback_flg is null and stock_reward_increase_flg is null
                and ifnull(change_flg,0) != 4 and supervision_reason is null
            then 'テクニカル'
        end as suggest_type,
        case 
            when 
                rsi <90 and volume_ratio >= 30 and roc >= -7.5 and roc < 2.5
                and short_envelope >= 0.95 and envelope >= 0.95
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.025
                and forcast_win_rate >= 0.9 and forcast_lose_rate < 0.025
                and reward_rate >= 0.04
            then 1
            when
                rsi > 0 and psychological >= 20 and roc >= -7.5 and roc < 2.5
                and short_envelope >= 0.95 and envelope >= 0.95 and long_envelope >= 0.95
                and top_relative_rate >= 0.4 and day60_top_relative_rate >= 0.8
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.025
                and forcast_win_rate >= 0.9 and forcast_lose_rate < 0.05
                and weather in(4,5)
            then 2
            when
                volume_ratio >= 30 and psychological < 60
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.025
                and forcast_win_rate >= 0.9 and forcast_lose_rate < 0.025
                and stock_split in(2,3)
            then 3
            when
                rsi < 70 and volume_ratio >= 60 and volume_ratio <220 and psychological < 60
                and roc >= -7.5 and roc < 5 and rci < 75
                and short_envelope >= 0.95 and envelope >= 0.95
                and bottom_relative_rate < 1.1 and day60_bottom_relative_rate < 1.1
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.2
                and forcast_win_rate >= 0.95 and forcast_lose_rate < 0.025
                and volatility <= 4 
            then 4
            when
                rsi >= 20 and volume_ratio >= 30 and volume_ratio < 250 and psychological >= 20 and psychological < 80
                and roc >= -7.5 and roc < 5 
                and short_envelope >= 0.95 and short_envelope < 1.01 and envelope >= 0.95 and envelope < 1.025
                and bottom_relative_rate is not null
                and forcast_up_rate >= 0.95 and forcast_down_rate < 0.025
                and forcast_win_rate >= 0.95 and forcast_lose_rate < 0.025
                and ifnull(pbr,0) < 10 and volatility <= 4 
                and buyback_flg is null and stock_reward_increase_flg is null
                and ifnull(change_flg,0) != 4 and supervision_reason is null
            then 5
        end as suggest_sort,
        date_diff(created_at,min_dt,day) as past_day
    from
        joint_tb
)
select
    *,
    count(stock_code) over(partition by stock_code,suggest_type order by past_day range between 120 preceding and current row) as dual_num
from
    suggest_add
)

