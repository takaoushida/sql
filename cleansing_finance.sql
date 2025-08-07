create or replace table temp_folder.cleansing_finance_suffix_date as
with
normalize as(--複数回提出したことになってる報告書があるのでdistinct必須
    select
        distinct
        * except(description,col_0,col_1,col_2,col_3,col_4,col_5,col_6,col_7,col_8,col_9),
        replace(replace(replace(normalize(description,NFKD),'元年','1年'),' ',''),'2202','2022') as description,--必ず西暦か年号が入る※4つだけ何も年についての記述がない
        replace(replace(replace(normalize(col_0,NFKD),',',''),'元年','1年'),' ','') as col_0,
        nullif(regexp_replace(replace(normalize(col_1,NFKD),',',''),r'[ 　─―-]',''),'') as col_1,
        nullif(regexp_replace(replace(normalize(col_2,NFKD),',',''),r'[ 　─―-]',''),'') as col_2,
        nullif(regexp_replace(replace(normalize(col_3,NFKD),',',''),r'[ 　─―-]',''),'') as col_3,
        nullif(regexp_replace(replace(normalize(col_4,NFKD),',',''),r'[ 　─―-]',''),'') as col_4,
        nullif(regexp_replace(replace(normalize(col_5,NFKD),',',''),r'[ 　─―-]',''),'') as col_5,
        nullif(regexp_replace(replace(normalize(col_6,NFKD),',',''),r'[ 　─―-]',''),'') as col_6,
        nullif(regexp_replace(replace(normalize(col_7,NFKD),',',''),r'[ 　─―-]',''),'') as col_7,
        nullif(regexp_replace(replace(normalize(col_8,NFKD),',',''),r'[ 　─―-]',''),'') as col_8,
        nullif(regexp_replace(replace(normalize(col_9,NFKD),',',''),r'[ 　─―-]',''),'') as col_9,
    from
        `quarterly_securities_report_raw.finance_*`
    where
        _table_suffix = 'suffix_date'
),
finance_flg_add as(--仕方なくearningsが混じってる短信があるのでそれを除外するためのフラグ立て
    select
        *,
        case when regexp_contains(col_1,r'資産|資本') then 1 end as finance_flg 
    from
        normalize
),
running_finance as(--一つ前のサブクエリだと当該行しか削除できないのでrunning
    select
        *,
        sum(finance_flg) over(partition by date,stock_code,description order by row_number) as running_finance_flg
    from
        finance_flg_add
),
finance_only as(--この段階ですでにcol_1は総資産、col_2は純資産
    select
        * except(finance_flg,running_finance_flg)
    from
        running_finance
    where
        running_finance_flg is not null
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
            when regexp_contains(col_0, r'\d{4}年') or regexp_contains(col_0, r'\d{4}月') or 
                 regexp_contains(col_0, r'\d{4}期') or regexp_contains(col_0, r'\d{4}n年') then '年'
            when regexp_contains(col_0,r'平成(\d+)年') or regexp_contains(col_0,r'平(\d+)') or regexp_contains(col_0,r'成(\d+)') then '平成'
            when regexp_contains(col_0,r'令和(\d+)年') then '令和'
        end as line_period_type,
        case
            when regexp_contains(col_0, r'\d{4}年') then regexp_extract(col_0,r'(\d{4})年') 
            when regexp_contains(col_0, r'\d{4}月') then regexp_extract(col_0,r'(\d{4})月')  --誤字対策
            when regexp_contains(col_0, r'\d{4}期') then regexp_extract(col_0,r'(\d{4})期')  --誤字対策
            when regexp_contains(col_0, r'\d{4}n年') then regexp_extract(col_0,r'(\d{4})n年')  --誤字対策
            when regexp_contains(col_0,r'平成(\d+)') then regexp_extract(col_0,r'平成(\d+)')  --誤字対策
            when regexp_contains(col_0,r'平(\d+)') then regexp_extract(col_0,r'平(\d+)')  --誤字対策
            when regexp_contains(col_0,r'成(\d+)') then regexp_extract(col_0,r'成(\d+)')  --誤字対策
            when regexp_contains(col_0,r'令和(\d+)') then regexp_extract(col_0,r'令和(\d+)')  --誤字対策
            when regexp_contains(col_0,r'第(\d+)期') then regexp_extract(col_0,r'第(\d+)期') --誤字対策
            when regexp_contains(col_0, r'(\d+)年') then regexp_extract(col_0,r'(\d+)年') --年号を略しているケース
        end as line_period_str,        
        case 
            when regexp_contains(col_0,r'第(\d+)四半期') then cast(regexp_extract(col_0,r'第(\d+)四半期') as int64)
            when col_0 not like '%四半期%' then 4
        end as line_quarter
    from
        finance_only
),
period_cast as(
    select
        * except(period_str,line_period_str),
        cast(period_str as int64) as period,
        cast(line_period_str as int64) as line_period
    from
        period_add
),
ad_add as(
    select
        *,
        case
            when period_type = '平成' then period + 1988
            when period_type = '令和' then period + 2018
            else period
        end as period_ad,
        case
            when line_period_type = '平成' then line_period + 1988
            when line_period_type = '令和' then line_period + 2018
            else line_period
        end as line_period_ad
    from
        period_cast
),
ad_confirm as(
    select
        *,
        case 
            when nullif(col_0,'') is null then null
            when period_ad is null then line_period_ad
            when period_ad - 2000 between line_period_ad -1 and line_period_ad +1 then line_period_ad +2000 --提出時の年+2000と誤差1年なら
            when line_period_ad < 20 then line_period_ad +2018 --令和
            when line_period_ad >= 1000 then line_period_ad --西暦
            else line_period_ad + 1988 --基本的に平成のみ
        end as line_ad_period,
    from
        ad_add
),
new_row_number_add as(
    select
        *,
        case 
            when line_ad_period is null then null
            else row_number() over(partition by date,description,stock_code order by line_ad_period desc) 
        end as new_row_number
    from
        ad_confirm
),
header as (
    select * from new_row_number_add where row_number = 1
),
data as(
    select * from new_row_number_add where new_row_number = 1
),
confirm_tb as(
select
    t1.date,
    t1.description,
    t1.stock_code,
    t1.col_1 as total_assets,
    t1.col_2 as net_assets,
    case
        when t2.col_3 in('自己資本比率','親会社所有者帰属持分比率','親会社の所有者に帰属する持分比率','当社株主帰属持分比率','自己資本規制比率') then t1.col_3
        when t2.col_4 in('自己資本比率','親会社所有者帰属持分比率','親会社の所有者に帰属する持分比率','当社株主帰属持分比率','自己資本規制比率') then t1.col_4
        when t2.col_5 in('自己資本比率','親会社所有者帰属持分比率','親会社の所有者に帰属する持分比率','当社株主帰属持分比率','自己資本規制比率') then t1.col_5
    end as equity_ratio
from
    data as t1
inner join
    header as t2
    on t1.date = t2.date and t1.description = t2.description and t1.stock_code = t2.stock_code
),
cast_tb as(
    select
        date,
        description,
        stock_code,
--        total_assets as total_assets_str,
--        net_assets as net_assets_str,
--        equity_ratio as equity_ratio_str,
        cast(replace(total_assets,'△','') as int64) as total_assets,
        cast(replace(net_assets,'△','') as int64) as net_assets,
        cast(replace(replace(regexp_replace(equity_ratio,r'\(.*\)', ''),'△',''),'%','') as float64) as equity_ratio
    from
        confirm_tb
)
select * from cast_tb