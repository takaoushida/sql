create or replace table securities_report.quartely_report_for_bi
partition by created_at
cluster by code,name as
(
with
window_tb as(
    select
        *,
        min(date) over(partition by stock_code,period,quarter) as created_at,
        row_number() over(partition by stock_code,period,quarter order by date desc,refine_flg desc) as row_number
    from
        `securities_report.after_cleanging_quartely_report_*`
    where --何も入っていないのはおかしな短信
        earnings is not null or operating_income is not null or ordinaly_profit is not null or net_income is not null or total_assets is not null or net_assets is not null
        or equity_ratio is not null or stock_amount is not null or stock_reward is not null
),
row_filtered as(
    select
        * except(date,refine_flg,row_number),
    from
        window_tb
    where 
        row_number = 1
),
quater_lag_add as(
    select
        *,
        case
            when quarter = 1 then earnings
            else earnings - lag(earnings,1) over(partition by stock_code,period order by quarter) 
        end as quarter_earnings,--earningsは年度累計のため、累計じゃない値に戻す
        case
            when quarter = 1 then operating_income
            else operating_income - lag(operating_income,1) over(partition by stock_code,period order by quarter) 
        end as quarter_operating_income,--operating_incomeは年度累計のため、累計じゃない値に戻す
        case
            when quarter = 1 then ordinaly_profit
            else ordinaly_profit - lag(ordinaly_profit,1) over(partition by stock_code,period order by quarter) 
        end as quarter_ordinaly_profit,--ordinaly_profitは年度累計のため、累計じゃない値に戻す
        case
            when quarter = 1 then net_income
            else net_income - lag(net_income,1) over(partition by stock_code,period order by quarter) 
        end as quarter_net_income,--net_incomeは年度累計のため、累計じゃない値に戻す
    from
        row_filtered
),
period_lag_add as(
    select
        *,
        lag(quarter_earnings,1) over(partition by stock_code,quarter order by period) as before_period_earnings,--前年同期
        lag(quarter_operating_income,1) over(partition by stock_code,quarter order by period) as before_period_operating_income,
        lag(quarter_ordinaly_profit,1) over(partition by stock_code,quarter order by period) as before_period_ordinaly_profit,
        lag(quarter_net_income,1) over(partition by stock_code,quarter order by period) as before_period_net_income,
    from
        quater_lag_add
),
aggre_data as(
    select
        * except(before_period_earnings,before_period_operating_income,before_period_ordinaly_profit,before_period_net_income),
        (quarter_earnings - before_period_earnings) / nullif(abs(before_period_earnings),0) as quarter_earnings_rate, --(今年の値-前年の値)/前年の値だが、前年が赤字だとマイナスになるのでabs
        (quarter_operating_income - before_period_operating_income) / nullif(abs(before_period_operating_income),0) as quarter_operating_income_rate,
        (quarter_ordinaly_profit - before_period_ordinaly_profit) / nullif(abs(before_period_ordinaly_profit),0) as quarter_ordinaly_profit_rate,
        (quarter_net_income - before_period_net_income) / nullif(abs(before_period_net_income),0) as quarter_net_income_rate,
        created_at = max(created_at) over(partition by stock_code) as last_bool
    from
        period_lag_add
)
select
    cast(created_at as date) as created_at,
    t2.code,
    t2.name,
    t1.* except(created_at,stock_code)
from
    aggre_data as t1
inner join
    stock_data_mst.stock_data_mst_tokyo_01 as t2
    on t1.stock_code = t2.code
)