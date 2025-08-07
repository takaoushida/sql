create or replace table temp_folder.cleansing_stock_reward_suffix_date as

with
normalize as(
    select
        * except(description,col_0,col_1,col_2,col_3,col_4,col_5,col_6,col_7,col_8),
        replace(replace(replace(normalize(description,NFKD),'元年','1年'),' ',''),'2202','2022') as description,--必ず西暦か年号が入る※4つだけ何も年についての記述がない
        replace(replace(replace(normalize(col_0,NFKD),',',''),'元年','1年'),' ','') as col_0,
        nullif(regexp_replace(replace(normalize(col_1,NFKD),',',''),r'[ 　─―-]',''),'') as col_1,
        nullif(regexp_replace(replace(normalize(col_2,NFKD),',',''),r'[ 　─―-]',''),'') as col_2,
        nullif(regexp_replace(replace(normalize(col_3,NFKD),',',''),r'[ 　─―-]',''),'') as col_3,
        nullif(regexp_replace(replace(normalize(col_4,NFKD),',',''),r'[ 　─―-]',''),'') as col_4,
        nullif(regexp_replace(replace(normalize(col_5,NFKD),',',''),r'[ 　─―-]',''),'') as col_5,
        nullif(regexp_replace(replace(normalize(col_6,NFKD),',',''),r'[ 　─―-]',''),'') as col_6,
        nullif(regexp_replace(replace(normalize(col_7,NFKD),',',''),r'[ 　─―-]',''),'') as col_7,
        nullif(regexp_replace(replace(normalize(col_8,NFKD),',',''),r'[ 　─―-]',''),'') as col_8
    from
        `quarterly_securities_report_raw.stock_reward_*`
    where
        _table_suffix = 'suffix_date'
        and length(col_0) < 100
),
move_tb as(
    select
        * except(col_0,col_1,col_2,col_3,col_4,col_5,col_6,col_7,col_8),
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
        case when description like '%変更%' then 1 end as refine_flg,
        case--四半期がついた短信は予想,予定は含んではならない
            when regexp_contains(col_0,r'普通株式|株式分割|現時点で未定|年間配当金合計|参考｜配当金の内訳|分割|記念配当|特別配当|当社株式に対する配当金|資本剰余金を配当原資') then null
            else regexp_extract(col_0, r'^(\d+)年') 
        end as line_year,
        case--決算期の変更により同じ年が並ぶことがあるので月も取得
            when regexp_contains(col_0,r'普通株式|株式分割|現時点で未定|年間配当金合計|参考｜配当金の内訳|分割|記念配当|特別配当|当社株式に対する配当金|資本剰余金を配当原資') then null
            else regexp_extract(col_0, r'(?:[1-4]|\d{2}|\d{4})年(\d{1,2})月期') 
        end as line_month,        
        case when regexp_contains(col_0,r'予想|予 想') then 1 end as plan_flg,
        --row_number = 2 はperiod,col_0が空なら正常だがそうでないなら1列前にずれてる
        case when row_number = 2 and col_0 like '%第1四半期%' then '' else col_0 end as col_0,--第1四半期末が本来だが未の会社がある
        case when row_number = 2 and col_0 != '' then col_0 else col_1 end as col_1,
        case when row_number = 2 and col_0 != '' then col_1 else col_2 end as col_2,
        case when row_number = 2 and col_0 != '' then col_2 else col_3 end as col_3,
        case when row_number = 2 and col_0 != '' then col_3 else col_4 end as col_4,
        case when row_number = 2 and col_0 != '' then col_4 else col_5 end as col_5,
        case when row_number = 2 and col_0 != '' then col_5 else col_6 end as col_6,
        case when row_number = 2 and col_0 != '' then col_6 else col_7 end as col_7,
        case when row_number = 2 and col_0 != '' then col_7 else col_8 end as col_8,
        case when regexp_contains(col_1, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}')  then 1 else 0 end +
        case when regexp_contains(col_2, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}')  then 1 else 0 end +
        case when regexp_contains(col_3, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}')  then 1 else 0 end +
        case when regexp_contains(col_4, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}')  then 1 else 0 end +
        case when regexp_contains(col_5, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}')  then 1 else 0 end +
        case when regexp_contains(col_6, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}')  then 1 else 0 end +
        case when regexp_contains(col_7, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}')  then 1 else 0 end +
        case when regexp_contains(col_8, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}')  then 1 else 0 end
        as header_flg
    from
        normalize
),
cast_tb as(
    select 
        * except(line_year,period_str,line_month),
        cast(period_str as int64) as period,
        cast(line_year as int64) as line_year,
        cast(line_month as int64) as line_month
    from
        move_tb
),
confirm_line_year_add as(--年末の短信は予想は来季の予想になるためomit
    select
        * ,
        case when quarter_str is null and plan_flg = 1 then null else line_year end as confirm_line_year,--四半期がつかない短信は予定を含まない
        sum(header_flg) over(partition by date,description,stock_code order by row_number) as header_num
     from
        cast_tb
),
aggre_year_add as(--四半期は同年の値と同年の予想とがあり、同年の値は空となる。予想に支払われるだろう配当が入るので+1してあげる
    select
        *,
        case when quarter_str is not null and plan_flg = 1 then confirm_line_year +1 else confirm_line_year end as aggre_year
    from
        confirm_line_year_add
),
max_line_year_add as(--行で1年前や予想の行があるので予想以外の最大の行を取得するためmaxを付与
    select
        *,
        case
            when period_type = '平成' then period + 1988
            when period_type = '令和' then period + 2018
            else period
        end as period_ad,
        case 
            when period - 2000 = confirm_line_year then period --提出時の西暦の下二桁と一致するなら提出時の西暦を正とする
            when confirm_line_year < 20 then confirm_line_year +2018 --令和
            when confirm_line_year >= 1000 then confirm_line_year --西暦
            else confirm_line_year + 1988 --基本的に平成のみ
        end as confirm_line_year_ad,
        row_number() over(partition by date,stock_code,description order by aggre_year desc,line_month desc) as latest_flg
    from
        aggre_year_add
),
outrange_cut as(--2年先まで予想を出しているケースがあるので除外
    select
        *
    from
        max_line_year_add
    where
        period_ad +1 >= confirm_line_year_ad
),
line_year_max_only as(--最新年のみにする
    select
        * 
    from 
        outrange_cut
    where
        latest_flg = 1
),
header as (
    select * from move_tb where row_number = 2
),
total_get as(
    select
        t1.date,
        t1.description,
        t1.stock_code,
        case
            when t2.col_5 = '合計' then t1.col_5 
            when t2.col_6 = '合計' then t1.col_6 
            when t2.col_7 = '合計' then t1.col_7 
        end as stock_reward_str,
    from
        line_year_max_only as t1
    inner join
        header as t2
        on t1.date = t2.date and t1.description = t2.description and t1.stock_code = t2.stock_code
),
data as(
select
    * except(stock_reward_str),
    case 
        when stock_reward_str in ('未定','未確定') then null
        when regexp_contains(stock_reward_str, r'(\d+\.\d+|\d+)~') then cast(regexp_extract(stock_reward_str, r'(\d+\.\d+|\d+)~') as float64)
        else cast(replace(stock_reward_str,'(最低額)','') as float64)
    end as stock_reward
from
    total_get
),
stock_amount as(
    select * from temp_folder.cleansing_stock_amount_suffix_date

)
select 
    t1.* except(stock_reward),
    stock_reward / t2.units as stock_reward  
from 
    data as t1
left join
    stock_amount as t2
    on t1.stock_code = t2.stock_code and t1.date = t2.date and t1.description = t2.description
