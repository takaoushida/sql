create or replace table securities_report.after_cleanging_quartely_report_suffix_date as
with
normalize as(
    select
        * except(description),
        replace(replace(replace(normalize(description,NFKD),'元年','1年'),' ',''),'2202','2022') as description
    from
        `quarterly_securities_report_raw.master_*`
    where
        _table_suffix = 'suffix_date'
),
period_add as(
    select
        *,
        case
            when regexp_contains(description, r'\d{4}年') or regexp_contains(description, r'\d{4}月') or 
                 regexp_contains(description, r'\d{4}期') or regexp_contains(description, r'\d{4}n年') then '年'
            when regexp_contains(description,r'平成(\d+)年') or regexp_contains(description,r'平(\d+)') or regexp_contains(description,r'成(\d+)') then '平成'
            when regexp_contains(description,r'令和(\d+)年') then '令和'
        end as period_type,
        case
            when regexp_contains(description, r'\d{4}年') then regexp_extract(description,r'(\d{4})年') 
            when regexp_contains(description, r'\d{4}月') then regexp_extract(description,r'(\d{4})月')  
            when regexp_contains(description, r'\d{4}期') then regexp_extract(description,r'(\d{4})期') 
            when regexp_contains(description, r'\d{4}n年') then regexp_extract(description,r'(\d{4})n年') 
            when regexp_contains(description,r'平成(\d+)') then regexp_extract(description,r'平成(\d+)') 
            when regexp_contains(description,r'平(\d+)') then regexp_extract(description,r'平(\d+)') 
            when regexp_contains(description,r'成(\d+)') then regexp_extract(description,r'成(\d+)') 
            when regexp_contains(description,r'令和(\d+)') then regexp_extract(description,r'令和(\d+)') 
            when regexp_contains(description,r'第(\d+)期') then regexp_extract(description,r'第(\d+)期')
        end as period_str,
        case
            when regexp_contains(description, r'第(\d+)四半期') then regexp_extract(description,r'第(\d+)四半期')
        end as quarter_str,
        case when regexp_contains(description,r'変更|訂正') then 1 end as refine_flg
    from
        normalize
),
period_cast as(
    select
        * except(period_str,quarter_str),
        cast(period_str as int64) as period,
        cast(quarter_str as int64) as quarter,
    from
        period_add
),
ad_add as(
    select
        date,
        description,
        stock_code,
        refine_flg,
        case
            when period_type = '平成' then period + 1988
            when period_type = '令和' then period + 2018
            when period is null then cast(format_date('%Y',cast(date as date)) as int64)
            else period
        end as period,
        case when quarter is null then 4 else quarter end as quarter
    from
        period_cast
)
select
    t1.* except(description),
    t2.* except(date,description,stock_code),
    t3.* except(date,description,stock_code),
    t4.* except(date,description,stock_code),
    t5.* except(date,description,stock_code)
from
    ad_add as t1
left join
    temp_folder.cleansing_earnings_suffix_date as t2
    on t1.date = t2.date and t1.description = t2.description and t1.stock_code = t2.stock_code
left join
    temp_folder.cleansing_finance_suffix_date as t3
    on t1.date = t3.date and t1.description = t3.description and t1.stock_code = t3.stock_code
left join
    temp_folder.cleansing_stock_amount_suffix_date as t4
    on t1.date = t4.date and t1.description = t4.description and t1.stock_code = t4.stock_code
left join
    temp_folder.cleansing_stock_reward_suffix_date as t5
    on t1.date = t5.date and t1.description = t5.description and t1.stock_code = t5.stock_code
order by stock_code,period,quarter