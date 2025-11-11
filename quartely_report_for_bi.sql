create or replace table securities_report.quartely_report_for_bi 
partition by release_date
cluster by stock_code as(
with
num_add as(--修正を含め四半期ごとの最終行を取得
    select
        *,
        row_number() over(partition by stock_code,period,quarter order by release_date desc,refine_flg desc) as row_number
    from
        securities_report.quartely_report_for_learning
),
final_data as(--最終的なperiod,quarterの値のみにする
    select
        * 
    from
        num_add
    where
        row_number = 1
),
exp_ln_base as(--株式分割を結合
    select
        t1.*,
        (t2.split_stock_amount + t2.exist_stock_amount) / t2.exist_stock_amount as split_rate,
    from
        final_data as t1
    left join
        `stock_data.stock_split_*` as t2
        on t1.stock_code = t2.stock_code and t2.split_date between t1.join_start_date and t1.join_end_date
),
daily_change as(--決算短信公開日間の分割を公開日に合わせる処理
    select
        stock_code,
        release_date,
        EXP(SUM(LN(IFNULL(split_rate,1)))) AS split_rate
    from
        exp_ln_base
    group by 1,2
),
cum_split_add as(--逆累積にし、当時の1株が今の何株になるかを集計
    select
        t1.*,
        EXP(SUM(LOG(IFNULL(t2.split_rate,1))) OVER (
            PARTITION BY t1.stock_code
            ORDER BY t1.release_date desc
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS cum_split_rate --利回りを調整後利回りにするための累積値
    from
        final_data as t1
    left join
        daily_change as t2
        on t1.stock_code = t2.stock_code and t1.release_date = t2.release_date
),
lag_add as(--lagを付与しつつ調整後配当を取得
    select
        *,
        lag(earnings,1) over(partition by stock_code,period order by quarter) as lag_earnings,
        lag(operating_income,1) over(partition by stock_code,period order by quarter) as lag_operating_income,
        lag(ordinaly_profit,1) over(partition by stock_code,period order by quarter) as lag_ordinaly_profit,
        lag(net_income,1) over(partition by stock_code,period order by quarter) as lag_net_income,
        lag(before_earnings,1) over(partition by stock_code,period order by quarter) as lag_before_earnings,
        lag(before_operating_income,1) over(partition by stock_code,period order by quarter) as lag_before_operating_income,
        lag(before_ordinaly_profit,1) over(partition by stock_code,period order by quarter) as lag_before_ordinaly_profit,
        lag(before_net_income,1) over(partition by stock_code,period order by quarter) as lag_before_net_income,
        stock_reward / cum_split_rate as refine_stock_reward
    from
        cum_split_add
),
quarter_value as(
    select
        *,
        case 
            when quarter = 1 then earnings
            else earnings - lag_earnings
        end as quarter_earnings,
        case 
            when quarter = 1 then operating_income
            else operating_income - lag_operating_income
        end as quarter_operating_income,
        case 
            when quarter = 1 then ordinaly_profit
            else ordinaly_profit - lag_ordinaly_profit
        end as quarter_ordinaly_profit,
        case 
            when quarter = 1 then net_income
            else net_income - lag_net_income
        end as quarter_net_income,
        case 
            when quarter = 1 then before_earnings
            else before_earnings - lag_before_earnings
        end as quarter_before_earnings,
        case 
            when quarter = 1 then before_operating_income
            else before_operating_income - lag_before_operating_income
        end as quarter_before_operating_income,
        case 
            when quarter = 1 then before_ordinaly_profit
            else before_ordinaly_profit - lag_before_ordinaly_profit
        end as quarter_before_ordinaly_profit,
        case 
            when quarter = 1 then before_net_income
            else before_net_income - lag_before_net_income
        end as quarter_before_net_income
    from
        lag_add
)
select
    release_date,
    stock_code,
    period,
    quarter,
    total_assets,--総資産
    net_assets,--純資産
    stock_amount,--発行済株式数
    stock_reward,--配当
    refine_stock_reward,--調整後配当
    earnings,--売上(累積)
    operating_income,--営業利益(累積)
    ordinaly_profit,--経常利益(累積)
    net_income,--純利益(累積)
    quarter_earnings,--売上(四半期)
    quarter_operating_income,--営業利益(四半期)
    quarter_ordinaly_profit,--経常利益(四半期)
    quarter_net_income,--純利益(四半期)
    (earnings - before_earnings)/ nullif(abs(before_earnings),0) as earnings_rate,--前年同期比売上(累積)
    (operating_income - before_operating_income)/ nullif(abs(before_operating_income),0) as operating_income_rate,--前年同期比営業利益(累積)
    (ordinaly_profit - before_ordinaly_profit) / nullif(abs(before_ordinaly_profit),0) as ordinaly_profit_rate,--前年同期比常利益(累積)
    (net_income - before_net_income) / nullif(abs(before_net_income),0) as net_income_rate,--前年同期比純利益(累積)
    (quarter_earnings - quarter_before_earnings) / nullif(abs(quarter_before_earnings),0) as quarter_earnings_rate,--前年同期比売上(四半期)
    (quarter_operating_income - quarter_before_operating_income) / nullif(abs(quarter_before_operating_income),0) as quarter_operating_income_rate,--前年同期比営業利益(四半期)
    (quarter_ordinaly_profit - quarter_before_ordinaly_profit) / nullif(abs(quarter_before_ordinaly_profit),0) as quarter_ordinaly_profit_rate,--前年同期比常利益(四半期)
    (quarter_net_income - quarter_before_net_income) / nullif(abs(quarter_before_net_income),0) as quarter_net_income_rate,--前年同期比純利益(四半期)
    quarter_net_income * 4 / nullif(total_assets,0) as roa,--ROA(総資産利益率)
    quarter_net_income * 4 / nullif(net_assets,0) as roe, --ROE(自己資本利益率)
    net_assets / nullif(total_assets,0) as equity_ratio,--自己資本比率
    (stock_reward * stock_amount) / nullif(net_income*1000000,0) reward_payout_ratio,--配当性向
    (refine_stock_reward * stock_amount) / nullif((net_income*1000000),0) as  refine_reward_payout_ratio,--調整後配当性向
    count(case when net_income < 0 then stock_code end) over(partition by stock_code order by period,quarter rows between 12 preceding and current row) as year3_red_count
from
    quarter_value
)
