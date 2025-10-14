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
)
select
    * 
from
    num_add
where
    row_number = 1
)
