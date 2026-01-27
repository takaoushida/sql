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
        min(t1.created_at) over(partition by t1.stock_code) as min_dt
    from
        stock_data  as t1
    left join
        temp_folder.feature_learning_up as t2
        on t1.stock_code = t2.stock_code and t1.created_at = t2.created_at
    left join
        temp_folder.feature_learning_down as t3
        on t1.stock_code = t3.stock_code and t1.created_at = t3.created_at
),
suggest_add as(
    select
        *,
        case
            when
                moving_avg >= 2 and rsi >= 40
                and volume_ratio >= 30 and volume_ratio < 500
                and psychological < 80
                and roc >= -5 and roc < 5
                and short_envelope >= 0.975 and short_envelope < 1.025 
                and envelope >= 0.95  and envelope < 1.025   
                and top_relative_rate >= 0.2
                and day60_top_relative_rate >= 0.7
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.1
                and ifnull(roa,0) < 0.2 
                and pbr >= 0 and pbr < 20
                and market_volatility < 0.02 and market_breath >= 0.3
                and reward_rate >= 0.04
                and price_range < 5
                and buyback_flg is null and stock_reward_increase_flg is null
                and irregular_flg is null
            then '高配当'
            when
                moving_avg != 4 
                and rsi >= 30 and rsi < 80
                and psychological >= 10
                and roc >= -5 and roc < 5
                and short_envelope >= 0.975 and short_envelope < 1.025 
                and envelope >= 0.95  and envelope < 1.025   
                and top_relative_rate >= 0.3
                and day60_top_relative_rate >= 0.6
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.1
                and quarter_net_income_rate >= -1.5 --純利益増減率
                and roa >= -0.03 and roa < 0.2
                and pbr < 10
                and market_volatility < 0.04
                and market_return >= -0.01
                and weather in(4,5)
                and price_range < 5
                and market_cap_section != 'large'
                and increase_num >= -1
                and buyback_flg is null and stock_reward_increase_flg is null
                and irregular_flg is null
            then '優良銘柄'
            when
                moving_avg in (1,2,3)
                and roc >= -7.5
                and short_envelope >= 0.975 and short_envelope < 1.025 
                and envelope >= 0.95  and envelope < 1.025   
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.1
                and volatility in(4,5)
                and (ipo_flg is null or ipo_flg = 3)
                and stock_split in (2,3)
            then '株式分割'
            when
                moving_avg in (2,3,4)
                and rsi >= 20 and rsi < 90
                and volume_ratio >= 60 and volume_ratio < 500
                and psychological >= 20 and psychological < 80
                and roc >= -5 and roc < 2.5
                and rci < 75
                and short_envelope >= 0.975 and short_envelope < 1.025 
                and envelope >= 0.975  and envelope < 1.025   
                and long_envelope < 1.025
                and top_relative_rate >= 0.2 and top_relative_rate < 0.8
                and day60_bottom_relative_rate < 1.3
                and day60_top_relative_rate >= 0.6
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.1
                and volatility = 4
                and roa < 0.2 
                and market_volatility < 0.04
                and market_breath >= 0.3 and market_breath <0.6
                and market_return >= -0.005 
                and price_range != 5
                and buyback_flg is null and stock_reward_increase_flg is null
                and change_flg is null
                and irregular_flg is null
                and supervision_reason is null
            then 'テクニカル'
        end as suggest_type,
        case
            when
                moving_avg >= 2 and rsi >= 40
                and volume_ratio >= 30 and volume_ratio < 500
                and psychological < 80
                and roc >= -5 and roc < 5
                and short_envelope >= 0.975 and short_envelope < 1.025 
                and envelope >= 0.95  and envelope < 1.025   
                and top_relative_rate >= 0.2
                and day60_top_relative_rate >= 0.7
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.1
                and ifnull(roa,0) < 0.2 
                and pbr >= 0 and pbr < 20
                and market_volatility < 0.02 and market_breath >= 0.3
                and reward_rate >= 0.04
                and price_range < 5
                and buyback_flg is null and stock_reward_increase_flg is null
                and irregular_flg is null
            then 1
            when
                moving_avg != 4 
                and rsi >= 30 and rsi < 80
                and psychological >= 10
                and roc >= -5 and roc < 5
                and short_envelope >= 0.975 and short_envelope < 1.025 
                and envelope >= 0.95  and envelope < 1.025   
                and top_relative_rate >= 0.3
                and day60_top_relative_rate >= 0.6
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.1
                and quarter_net_income_rate >= -1.5 --純利益増減率
                and roa >= -0.03 and roa < 0.2
                and pbr < 10
                and market_volatility < 0.04
                and market_return >= -0.01
                and weather in(4,5)
                and price_range < 5
                and market_cap_section != 'large'
                and increase_num >= -1
                and buyback_flg is null and stock_reward_increase_flg is null
                and irregular_flg is null
            then 2
            when
                moving_avg in (1,2,3)
                and roc >= -7.5
                and short_envelope >= 0.975 and short_envelope < 1.025 
                and envelope >= 0.95  and envelope < 1.025   
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.1
                and volatility in(4,5)
                and (ipo_flg is null or ipo_flg = 3)
                and stock_split in (2,3)
            then 3
            when
                moving_avg in (2,3,4)
                and rsi >= 20 and rsi < 90
                and volume_ratio >= 60 and volume_ratio < 500
                and psychological >= 20 and psychological < 80
                and roc >= -5 and roc < 2.5
                and rci < 75
                and short_envelope >= 0.975 and short_envelope < 1.025 
                and envelope >= 0.975  and envelope < 1.025   
                and long_envelope < 1.025
                and top_relative_rate >= 0.2 and top_relative_rate < 0.8
                and day60_bottom_relative_rate < 1.3
                and day60_top_relative_rate >= 0.6
                and forcast_up_rate >= 0.9 and forcast_down_rate < 0.1
                and volatility = 4
                and roa < 0.2 
                and market_volatility < 0.04
                and market_breath >= 0.3 and market_breath <0.6
                and market_return >= -0.005 
                and price_range != 5
                and buyback_flg is null and stock_reward_increase_flg is null
                and change_flg is null
                and irregular_flg is null
                and supervision_reason is null
            then 4
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

