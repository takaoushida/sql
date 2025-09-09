create or replace table securities_report.quartely_report_for_learning as
with
row_number_add as(--joinとfilterのために2つの数列を付与
    select 
        *
    from
        `securities_report.after_cleanging_quartely_report_*`
    where --何も入っていないのはおかしな短信
        earnings is not null or operating_income is not null or ordinaly_profit is not null or net_income is not null or total_assets is not null or net_assets is not null
        or equity_ratio is not null or stock_amount is not null or stock_reward is not null
),
--直前の期を付与したいのに前年同期を付与している
self_join as(--1期前の値を付与　※通常と訂正の両方が付与される
    select
        t1.*,
        t2.earnings as before_earnings,
        t2.operating_income as before_operating_income,
        t2.ordinaly_profit as before_ordinaly_profit,
        t2.net_income as before_net_income,
        ifnull(row_number() over(partition by t2.stock_code order by t2.date,t2.refine_flg),0) as row_number2
    from
        row_number_add as t1
    left join
        row_number_add as t2
        on t1.stock_code = t2.stock_code and t1.period = t2.period and t1.quarter = t2.quarter +1
),
max_row_number_add as(--複数結合したうち、最新日付のものだけにする準備
    select
        *,
        max(row_number2) over(partition by stock_code,period,quarter) as max_row_number2
    from
        self_join
),
max_row_number_only as(--最新日付のものだけにするとともに累計を単期に変更,再度数列を付与
    select 
        * except(row_number2,max_row_number2,before_earnings,before_operating_income,before_ordinaly_profit,before_net_income),
        case
            when quarter = 1 then earnings
            else earnings - before_earnings
        end as quarter_earnings,  
        case
            when quarter = 1 then operating_income
            else operating_income - before_operating_income
        end as quarter_operating_income,
        case
            when quarter = 1 then ordinaly_profit
            else ordinaly_profit - before_ordinaly_profit
        end as quarter_ordinaly_profit,
        case
            when quarter = 1 then net_income
            else net_income - before_net_income
        end as quarter_net_income,
        dense_rank() over(partition by stock_code,quarter order by period) as quarter_row_number,
        row_number() over(partition by stock_code order by date) as row_number
    from 
        max_row_number_add
    where
        row_number2 = max_row_number2
),
reself_join as(--前年同期をjoinで付与するが、修正があった場合複数結合してしまう
    select
        t1.*,
        t2.quarter_earnings as before_period_earnings,
        t2.quarter_operating_income as before_period_operating_income,
        t2.quarter_ordinaly_profit as before_period_ordinaly_profit,
        t2.quarter_net_income as before_period_net_income,
        ifnull(t2.row_number,0) as row_number2
    from
        max_row_number_only as t1
    left join
        max_row_number_only as t2
        on t1.stock_code = t2.stock_code and t1.quarter = t2.quarter and t1.quarter_row_number = t2.quarter_row_number +1
),
remax_row_number_add as(--複数結合したうち、最新日付のものだけにする準備
    select
        *, 
        (quarter_operating_income / nullif(quarter_earnings,0)) / nullif((before_period_operating_income / nullif(before_period_earnings,0)),0) as quarter_operating_income_gain_rate,
        (quarter_earnings - before_period_earnings) / nullif(abs(before_period_earnings),0) as quarter_earnings_rate, --(今年の値-前年の値)/前年の値だが、前年が赤字だとマイナスになるのでabs
        (quarter_operating_income - before_period_operating_income) / nullif(abs(before_period_operating_income),0) as quarter_operating_income_rate,
        (quarter_ordinaly_profit - before_period_ordinaly_profit) / nullif(abs(before_period_ordinaly_profit),0) as quarter_ordinaly_profit_rate,
        (quarter_net_income - before_period_net_income) / nullif(abs(before_period_net_income),0) as quarter_net_income_rate,

        max(row_number2) over(partition by stock_code,period,quarter) as max_row_number2
    from
        reself_join
)
select 
    * except(quarter_row_number,row_number,row_number2,max_row_number2,before_period_earnings,before_period_operating_income,before_period_ordinaly_profit,before_period_net_income),
    cast(date as date) as release_date,
    ifnull(lead(cast(date as date),1) over(partition by stock_code order by date),date_add(current_date('Asia/Tokyo'),interval +1 day)) as next_release_date --次回の短信がないなら明日の日付※株価との結合時-1dayで結合するので1日多くしておく
from 
    remax_row_number_add
where
    row_number2 = max_row_number2
