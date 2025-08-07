create or replace table temp_folder.cleansing_stock_amount_suffix_date as
with
col_move as(
    select
        * except(description),
        replace(replace(replace(normalize(description,NFKD),'元年','1年'),' ',''),'2202','2022') as description,--必ず西暦か年号が入る※4つだけ何も年についての記述がない
        case when col_0 like '%口数%' then 100 else 1 end as units,
        case
            when col_1 = '' then col_3
            else col_2
        end as stock_amount_str
    from
        `quarterly_securities_report_raw.stock_amount_*`
    where
        _table_suffix = 'suffix_date'
),
cast_tb as(
    select
        date,
        description,
        stock_code,
        units,
        cast(nullif(regexp_replace(stock_amount_str,r'[.,口株―]',''),'') as int64) as stock_amount
    from
        col_move
    where
        row_number = 1    
)
select
    * except(stock_amount),
    units * stock_amount as stock_amount
from
    cast_tb
