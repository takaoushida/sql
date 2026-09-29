create or replace table feature_learning_prd.grouping_{target_date_suffix}
partition by created_at as(
    with
    tables as(
        select
            *,
            coalesce(mcs_small_bottom_relative_rate,mcs_mid_bottom_relative_rate,mcs_large_bottom_relative_rate) as mcs_bottom_relative_rate,
            coalesce(mcs_small_top_relative_rate,mcs_mid_top_relative_rate,mcs_large_top_relative_rate) as mcs_top_relative_rate
        from
            looker_datamart.stock_data_explanatory_valiable_add
        where
            created_at = '{target_date_str}'
    )
    select
        t1.created_at,
        t1.stock_code,
        moving_avg,
        stocasticks,
        case
            when rsi = 0 then 0
            when rsi < 10 then 1
            when rsi < 20 then 2
            when rsi < 30 then 3
            when rsi < 40 then 4
            when rsi < 50 then 5
            when rsi < 60 then 6
            when rsi < 70 then 7
            when rsi < 80 then 8
            when rsi < 90 then 9
            when rsi >= 90 then 10
            else 11
        end as rsi,
        case
            when volume_ratio < 30 then  1
            when volume_ratio < 50 then  2
            when volume_ratio < 70 then  3
            when volume_ratio < 90 then  4
            when volume_ratio < 120 then 5
            when volume_ratio < 150 then 6
            when volume_ratio < 200 then 7
            when volume_ratio < 250 then 8
            when volume_ratio < 250 then 9
            when volume_ratio < 500 then 10
            when volume_ratio < 1000 then 11
            when volume_ratio >= 1000 then 12
            else 13
        end as volume_ratio,
        case
            when psychological < 10 then  1
            when psychological < 20 then  2
            when psychological < 30 then  3
            when psychological < 40 then  4
            when psychological < 50 then  5
            when psychological < 60 then  6
            when psychological < 70 then  7
            when psychological < 80 then  8
            when psychological < 90 then  9
            when psychological >= 90 then 10
            else 11
        end as psychological,
        case
            when roc < -10 then  1
            when roc < -7.5 then 2
            when roc < -5 then   3
            when roc < -2.5 then 4
            when roc < -1 then   5
            when roc < 0 then    6
            when roc < 1 then    7
            when roc < 2.5 then  8
            when roc < 5 then    9
            when roc < 7.5 then  10
            when roc < 10 then   11
            when roc >= 10 then  12     
            else 13
        end as roc,
        case
            when rci < -100 then 1
            when rci < -75 then  2
            when rci < -50 then  3
            when rci < -25 then  4
            when rci < 0 then    5
            when rci < 25 then   6
            when rci < 50 then   7
            when rci < 75 then   8
            when rci >= 75 then  9
            else 10
        end as rci,
        case
            when short_envelope < 0.95  then 1
            when short_envelope < 0.975 then 2
            when short_envelope < 0.99  then 3
            when short_envelope < 1     then 4
            when short_envelope < 1.01  then 5
            when short_envelope < 1.025 then 6
            when short_envelope < 1.5   then 7
            when short_envelope >= 1.5  then 8
            else 9
        end as short_envelope,
        case
            when envelope < 0.95  then 1
            when envelope < 0.975 then 2
            when envelope < 0.99  then 3
            when envelope < 1     then 4
            when envelope < 1.01  then 5
            when envelope < 1.025 then 6
            when envelope < 1.5   then 7
            when envelope >= 1.5  then 8
            else 9
        end as envelope,
        case
            when long_envelope < 0.95  then 1
            when long_envelope < 0.975 then 2
            when long_envelope < 0.99  then 3
            when long_envelope < 1     then 4
            when long_envelope < 1.01  then 5
            when long_envelope < 1.025 then 6
            when long_envelope < 1.5   then 7
            when long_envelope >= 1.5  then 8
            else 9
        end as long_envelope,
        case 
            when bottom_relative_rate < 1.05 then  1
            when bottom_relative_rate < 1.1  then  2
            when bottom_relative_rate < 1.2  then  3
            when bottom_relative_rate < 1.3  then  4
            when bottom_relative_rate < 1.4  then  5
            when bottom_relative_rate < 1.5  then  6    
            when bottom_relative_rate < 1.6  then  7    
            when bottom_relative_rate < 1.8  then  8    
            when bottom_relative_rate >= 1.8 then  9        
            else 10
        end as bottom_relative_rate,
        case
            when top_relative_rate < 0.2  then 1
            when top_relative_rate < 0.3  then 2
            when top_relative_rate < 0.4  then 3
            when top_relative_rate < 0.5  then 4
            when top_relative_rate < 0.6  then 5
            when top_relative_rate < 0.7  then 6    
            when top_relative_rate < 0.8  then 7    
            when top_relative_rate < 0.9  then 8    
            when top_relative_rate >= 0.9 then 9
            else 10
        end as top_relative_rate,
        case 
            when day60_bottom_relative_rate < 1.05 then  1
            when day60_bottom_relative_rate < 1.1  then  2
            when day60_bottom_relative_rate < 1.2  then  3
            when day60_bottom_relative_rate < 1.3  then  4
            when day60_bottom_relative_rate < 1.4  then  5
            when day60_bottom_relative_rate < 1.5  then  6    
            when day60_bottom_relative_rate < 1.6  then  7    
            when day60_bottom_relative_rate < 1.8  then  8    
            when day60_bottom_relative_rate >= 1.8 then  9      
            else  10
        end as day60_bottom_relative_rate,
        case
            when day60_top_relative_rate < 0.2  then 1
            when day60_top_relative_rate < 0.3  then 2
            when day60_top_relative_rate < 0.4  then 3
            when day60_top_relative_rate < 0.5  then 4
            when day60_top_relative_rate < 0.6  then 5
            when day60_top_relative_rate < 0.7  then 6    
            when day60_top_relative_rate < 0.8  then 7    
            when day60_top_relative_rate < 0.9  then 8    
            when day60_top_relative_rate >= 0.9 then 9
            else 10
        end as day60_top_relative_rate,
        case
            when quarter_net_income_rate < -1.5 then  1
            when quarter_net_income_rate < -1   then  2    
            when quarter_net_income_rate < -0.5 then  3    
            when quarter_net_income_rate < -0.3 then  4   
            when quarter_net_income_rate < 0.0  then  5  
            when quarter_net_income_rate < 0.3  then  6 
            when quarter_net_income_rate < 0.5  then  7 
            when quarter_net_income_rate < 1    then  8 
            when quarter_net_income_rate < 1.5  then  9 
            when quarter_net_income_rate >= 1.5 then  10
            else 11
        end as quarter_net_income_rate,
        case 
            when roe < -0.1  then 1
            when roe < -0.05 then 2  
            when roe < -0.03 then 3  
            when roe < -0.01 then 4    
            when roe < 0     then 5    
            when roe < 0.01  then 6  
            when roe < 0.03  then 7
            when roe < 0.05  then 8    
            when roe < 0.07  then 9 
            when roe < 0.1   then 10 
            when roe < 0.2   then 11     
            when roe >= 0.2  then 12    
            else 13        
        end as roe,
        case
            when roa < -0.1  then 1
            when roa < -0.05 then 2    
            when roa < -0.03 then 3    
            when roa < -0.01 then 4    
            when roa < 0     then 5    
            when roa < 0.01  then 6  
            when roa < 0.03  then 7
            when roa < 0.05  then 8    
            when roa < 0.07  then 9 
            when roa < 0.1   then 10
            when roa < 0.2   then 11    
            when roa >= 0.2  then 12    
            else 13 
        end as roa,
        case
            when pbr < 0    then 1    
            when pbr < 0.25 then 2    
            when pbr < 0.5  then 3    
            when pbr < 0.7  then 4    
            when pbr < 1    then 5    
            when pbr < 2    then 6    
            when pbr < 3    then 7  
            when pbr < 5    then 8
            when pbr < 10   then 9    
            when pbr < 15   then 10 
            when pbr < 20   then 11 
            when pbr >= 20  then 12
            else 13
        end as pbr,
        price_range, --元々tier
        case
            when reward_rate < 0.01  then 1    
            when reward_rate < 0.02  then 2    
            when reward_rate < 0.03  then 3  
            when reward_rate < 0.04  then 4  
            when reward_rate < 0.05  then 5  
            when reward_rate >= 0.05 then 6    
            else 7
        end as reward_rate,             
        case
            when market_volatility < 0.014 then  1    
            when market_volatility < 0.015 then  2    
            when market_volatility < 0.016 then  3  
            when market_volatility < 0.017 then  4  
            when market_volatility < 0.018 then  5
            when market_volatility < 0.02  then  6  
            when market_volatility < 0.04  then  7  
            when market_volatility >= 0.04 then  8     
            else 9
        end as market_volatility,           
        case
            when market_breath < 0.2  then 1    
            when market_breath < 0.3  then 2    
            when market_breath < 0.4  then 3  
            when market_breath < 0.45 then 4  
            when market_breath < 0.5  then 5
            when market_breath < 0.55 then 6
            when market_breath < 0.6  then 7  
            when market_breath < 0.65 then 8
            when market_breath >= 0.6 then 9     
            else 10
        end as market_breath,
        case
            when market_return < -0.02   then 1    
            when market_return < -0.01   then 2    
            when market_return < -0.005  then 3    
            when market_return < -0.0025 then 4    
            when market_return < 0       then 5  
            when market_return < 0.0025  then 6  
            when market_return < 0.005   then 7
            when market_return < 0.01    then 8  
            when market_return < 0.02    then 9  
            when market_return >= 0.02   then 10
            else 11
        end as market_return,
        case
            when mcs_bottom_relative_rate < 1.025 then 1
            when mcs_bottom_relative_rate < 1.05  then 2
            when mcs_bottom_relative_rate < 1.075 then 3
            when mcs_bottom_relative_rate < 1.1   then 4
            when mcs_bottom_relative_rate < 1.2   then 5
            when mcs_bottom_relative_rate < 1.3   then 6
            when mcs_bottom_relative_rate >= 1.3  then 7        
            else 8
        end as mcs_bottom_relative_rate,   
        case
            when mcs_top_relative_rate < 0.7    then 1    
            when mcs_top_relative_rate < 0.8    then 2    
            when mcs_top_relative_rate < 0.85   then 3    
            when mcs_top_relative_rate < 0.9    then 4    
            when mcs_top_relative_rate < 0.95   then 5    
            when mcs_top_relative_rate < 0.975  then 6    
            when mcs_top_relative_rate >= 0.975 then 7    
            else 8              
        end as mcs_top_relative_rate,
        case 
            when day2_crease_rate < 0.9  then 1    
            when day2_crease_rate < 0.95 then 2    
            when day2_crease_rate < 0.98 then 3    
            when day2_crease_rate < 0.99 then 4    
            when day2_crease_rate < 1    then 5    
            when day2_crease_rate < 1.01 then 6    
            when day2_crease_rate < 1.02 then 7    
            when day2_crease_rate < 1.05 then 8    
            when day2_crease_rate < 1.1  then 9    
            when day2_crease_rate >= 1.1 then 10
            else 11
        end as day2_crease_rate,
        case 
            when day5_crease_rate < 0.9  then 1    
            when day5_crease_rate < 0.95 then 2    
            when day5_crease_rate < 0.98 then 3    
            when day5_crease_rate < 0.99 then 4    
            when day5_crease_rate < 1    then 5    
            when day5_crease_rate < 1.01 then 6    
            when day5_crease_rate < 1.02 then 7    
            when day5_crease_rate < 1.05 then 8    
            when day5_crease_rate < 1.1  then 9    
            when day5_crease_rate >= 1.1 then 10
            else 11
        end as day5_crease_rate, 
        case 
            when day20_crease_rate < 0.9  then 1    
            when day20_crease_rate < 0.95 then 2    
            when day20_crease_rate < 0.98 then 3    
            when day20_crease_rate < 0.99 then 4    
            when day20_crease_rate < 1    then 5    
            when day20_crease_rate < 1.01 then 6    
            when day20_crease_rate < 1.02 then 7    
            when day20_crease_rate < 1.05 then 8    
            when day20_crease_rate < 1.1  then 9    
            when day20_crease_rate >= 1.1 then 10
            else 11
        end as day20_crease_rate,               
        case 
            when day60_crease_rate < 0.9  then 1    
            when day60_crease_rate < 0.95 then 2    
            when day60_crease_rate < 0.98 then 3    
            when day60_crease_rate < 0.99 then 4    
            when day60_crease_rate < 1    then 5    
            when day60_crease_rate < 1.01 then 6    
            when day60_crease_rate < 1.02 then 7    
            when day60_crease_rate < 1.05 then 8    
            when day60_crease_rate < 1.1  then 9    
            when day60_crease_rate >= 1.1 then 10
            else 11
        end as day60_crease_rate,  
        case
            when net_income_annualized_ratio < -0.5 then 1  
            when net_income_annualized_ratio < -0.2 then 2  
            when net_income_annualized_ratio < -0.1 then 3  
            when net_income_annualized_ratio < 0    then 4  
            when net_income_annualized_ratio < 0.1  then 5  
            when net_income_annualized_ratio < 0.2  then 6  
            when net_income_annualized_ratio < 0.5  then 7  
            when net_income_annualized_ratio >= 0.5 then 8      
            else 9
        end as net_income_annualized_ratio,
        free_float_ratio_tier,--元々tier
        case
            when volatility < 0.0075 then 1    
            when volatility < 0.01   then 2    
            when volatility < 0.0125 then 3    
            when volatility < 0.015  then 4    
            when volatility < 0.0175  then 5    
            when volatility < 0.02   then 6    
            when volatility < 0.025  then 7    
            when volatility < 0.03   then 8    
            when volatility >= 0.03  then 9   
            else 10  
        end as volatility,   
        case 
            when net_income_rate < -1 then 1
            when net_income_rate < -0.5 then 2
            when net_income_rate < -0.25 then 3
            when net_income_rate < -0.1 then 4
            when net_income_rate < 0 then 5
            when net_income_rate < 0.1 then 6
            when net_income_rate < 0.25 then 7
            when net_income_rate < 0.5 then 8
            when net_income_rate < 1 then 9
            when net_income_rate >= 1 then 10
            else 11
        end as net_income_rate,  --net_income_gain_flgは削除
        t1.market_cap_section,
        cast(ifnull(t1.ipo_flg,0) as string) as ipo_flg,
        ifnull(t1.supervision_reason,'null') as supervision_reason,
        cast(ifnull(t1.increase_num,0) as string) as increase_num,
        cast(ifnull(t1.irregular_flg,0) as string) as irregular_flg,
        cast(ifnull(t1.change_flg,0) as string) as change_flg,
        cast(t1.year3_red_count as string) as year3_red_count,
        cast(ifnull(t1.stock_reward_increase_flg,0) as string) as stock_reward_increase_flg,
        t1.weather,
        cast(ifnull(t1.increase_past_day_tier,0) as string) as increase_past_day_tier,
        cast(ifnull(t1.decrease_past_day_tier,0) as string) as decrease_past_day_tier,
        t1.up_flg,
        t1.down_flg,
        t1.win_flg,
        t1.lose_flg,   
        cast(t1.volume_tier as string) as volume_tier,
        cast(ifnull(t1.buyback_flg,0) as string) as buyback_flg,
        cast(ifnull(t1.stock_split,0) as string) as stock_split,
        cast(ifnull(t1.quarter,99) as string) as quarter,
        cast(ifnull(t1.month,99) as string) as month,
        cast(ifnull(t1.q4_month_diff,99) as string) as q4_month_diff,
        case 
            when t2.forcast_up_rate < 0.1 then '1'
            when t2.forcast_up_rate < 0.2 then '2'
            when t2.forcast_up_rate < 0.3 then '3'
            when t2.forcast_up_rate < 0.4 then '4'
            when t2.forcast_up_rate < 0.5 then '5'
            when t2.forcast_up_rate < 0.6 then '6'
            when t2.forcast_up_rate < 0.7 then '7'
            when t2.forcast_up_rate < 0.8 then '8'
            when t2.forcast_up_rate < 0.9 then '9'
            when t2.forcast_up_rate >= 0.9 then '10'
        end as forcast_up_rate,
        case 
            when t3.forcast_down_rate < 0.1 then '1'
            when t3.forcast_down_rate < 0.2 then '2'
            when t3.forcast_down_rate < 0.3 then '3'
            when t3.forcast_down_rate < 0.4 then '4'
            when t3.forcast_down_rate < 0.5 then '5'
            when t3.forcast_down_rate < 0.6 then '6'
            when t3.forcast_down_rate < 0.7 then '7'
            when t3.forcast_down_rate < 0.8 then '8'
            when t3.forcast_down_rate < 0.9 then '9'
            when t3.forcast_down_rate >= 0.9 then '10'
        end as forcast_down_rate,
        cast(ifnull(t1.type1,0) as string) as type1,
    from
        tables as t1
    left join
        temp_folder.feature_learning_up as t2
        on t1.created_at = t2.created_at and t1.stock_code = t2.stock_code
    left join
        temp_folder.feature_learning_down as t3
        on t1.created_at = t3.created_at and t1.stock_code = t3.stock_code
) ;
