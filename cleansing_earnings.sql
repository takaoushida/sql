create or replace table temp_folder.cleansing_earnings_suffix_date as
with
normalize as(--複数回提出したことになってる報告書があるのでdistinct必須
    select
        distinct
        * except(description,col_0,col_1,col_2,col_3,col_4,col_5,col_6,col_7,col_8,col_9,col_10,col_11,col_12,col_13,col_14,col_15,col_16),
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
        nullif(regexp_replace(replace(normalize(col_10,NFKD),',',''),r'[ 　─―-]',''),'') as col_10,
        nullif(regexp_replace(replace(normalize(col_11,NFKD),',',''),r'[ 　─―-]',''),'') as col_11,
        nullif(regexp_replace(replace(normalize(col_12,NFKD),',',''),r'[ 　─―-]',''),'') as col_12,
        nullif(regexp_replace(replace(normalize(col_13,NFKD),',',''),r'[ 　─―-]',''),'') as col_13,
        nullif(regexp_replace(replace(normalize(col_14,NFKD),',',''),r'[ 　─―-]',''),'') as col_14,
        nullif(regexp_replace(replace(normalize(col_15,NFKD),',',''),r'[ 　─―-]',''),'') as col_15,
        nullif(regexp_replace(replace(normalize(col_16,NFKD),',',''),r'[ 　─―-]',''),'') as col_16,
    from
        `quarterly_securities_report_raw.earnings_*`
    where
        _table_suffix = 'suffix_date'
        and length(col_0) <= 50 and not regexp_contains(col_0,r'包括利益') and not regexp_contains(col_1,r'非継続事業')
),
move_tb as(
    select
        * except(description,col_0,col_1,col_2,col_3,col_4,col_5,col_6,col_7,col_8,col_9,col_10,col_11,col_12,col_13,col_14,col_15,col_16),
        case when regexp_contains(description, r'\d{5}年') then null else description end as description,--20220年といった具合にミスってるのがある
        --row_number = 2 は単位,col_0が空なら正常だがそうでないなら1列前にずれてる
        case when row_number = 2 and col_0 like '%第1四半期%' then '' else col_0 end as col_0,--第1四半期末が本来だが未の会社がある
        case when row_number = 2 and col_0 != '' then col_0 else col_1 end as col_1,
        case when row_number = 2 and col_0 != '' then col_1 else col_2 end as col_2,
        case when row_number = 2 and col_0 != '' then col_2 else col_3 end as col_3,
        case when row_number = 2 and col_0 != '' then col_3 else col_4 end as col_4,
        case when row_number = 2 and col_0 != '' then col_4 else col_5 end as col_5,
        case when row_number = 2 and col_0 != '' then col_5 else col_6 end as col_6,
        case when row_number = 2 and col_0 != '' then col_6 else col_7 end as col_7,
        case when row_number = 2 and col_0 != '' then col_7 else col_8 end as col_8,
        case when row_number = 2 and col_0 != '' then col_8 else col_9 end as col_9,
        case when row_number = 2 and col_0 != '' then col_9 else col_10 end as col_10,
        case when row_number = 2 and col_0 != '' then col_10 else col_11 end as col_11,
        case when row_number = 2 and col_0 != '' then col_11 else col_12 end as col_12,
        case when row_number = 2 and col_0 != '' then col_12 else col_13 end as col_13,
        case when row_number = 2 and col_0 != '' then col_13 else col_14 end as col_14,
        case when row_number = 2 and col_0 != '' then col_14 else col_15 end as col_15,
        case when row_number = 2 and col_0 != '' then col_15 else col_16 end as col_16
    from
        normalize
),
coa_base as(--見出しにコアベースとある報告書は2段目が欲しいので特定※カタカナじゃないのが含まれてる
    select distinct date,description,stock_code from move_tb
    where col_0 = 'コアベース'
),
unnecessary_wipe as(--headerの上に見出しがついている企業があるので削除
    select 
        * ,
        row_number() over(partition by date,description,stock_code,col_0,col_1 order by row_number) as col_0_row_number
    from
        move_tb
    where
        ifnull(col_1,'') not in('継続事業に係る金額') --col_1がnullだと除外されてしまうためifnull必須
),
coa_base_unnecessary_wipe as(--コアベースがある報告書は1行目を削除
    select 
        t1.* except(col_0_row_number)
    from
        unnecessary_wipe as t1
    left join
        coa_base as t2
        on t1.date = t2.date and t1.description = t2.description and t1.stock_code = t2.stock_code
    where
        t2.stock_code is null or (t2.stock_code is not null and col_0_row_number != 1)
),
text_flg_add as(--ヘッダー行を特定するためフラグ付与
    select
        *,
        case when (regexp_contains(col_1, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_1 not in('百万円','千米ドル')) or col_1 like '%EBITDA%' then 1 else 0 end +
        case when (regexp_contains(col_2, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_2 not in('百万円','千米ドル')) or col_2 like '%EBITDA%' then 1 else 0 end +
        case when (regexp_contains(col_3, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_3 not in('百万円','千米ドル')) or col_3 like '%EBITDA%' then 1 else 0 end +
        case when (regexp_contains(col_4, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_4 not in('百万円','千米ドル')) or col_4 like '%EBITDA%' then 1 else 0 end +
        case when (regexp_contains(col_5, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_5 not in('百万円','千米ドル')) or col_5 like '%EBITDA%' then 1 else 0 end +
        case when (regexp_contains(col_6, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_6 not in('百万円','千米ドル')) or col_6 like '%EBITDA%' then 1 else 0 end +
        case when (regexp_contains(col_7, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_7 not in('百万円','千米ドル')) or col_7 like '%EBITDA%' then 1 else 0 end +
        case when (regexp_contains(col_8, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_8 not in('百万円','千米ドル')) or col_8 like '%EBITDA%' then 1 else 0 end + 
        case when (regexp_contains(col_8, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_9 not in('百万円','千米ドル')) or col_9 like '%EBITDA%' then 1 else 0 end +
        case when (regexp_contains(col_8, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_10 not in('百万円','千米ドル')) or col_10 like '%EBITDA%' then 1 else 0 end +
        case when (regexp_contains(col_8, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_11 not in('百万円','千米ドル')) or col_11 like '%EBITDA%' then 1 else 0 end +
        case when (regexp_contains(col_8, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_12 not in('百万円','千米ドル')) or col_12 like '%EBITDA%' then 1 else 0 end +
        case when (regexp_contains(col_8, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_13 not in('百万円','千米ドル')) or col_13 like '%EBITDA%' then 1 else 0 end +
        case when (regexp_contains(col_8, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_14 not in('百万円','千米ドル')) or col_14 like '%EBITDA%' then 1 else 0 end +
        case when (regexp_contains(col_8, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_15 not in('百万円','千米ドル')) or col_15 like '%EBITDA%' then 1 else 0 end +
        case when (regexp_contains(col_8, r'[ぁ-んァ-ヶ一-龥々〆ヵ]{3,}') and col_16 not in('百万円','千米ドル')) or col_16 like '%EBITDA%' then 1 else 0 end 
        as text_flg,
    from
        coa_base_unnecessary_wipe      
),
header_add as(--本来のヘッダーの上に見出しが別途ついているケースがあるのでrow_number毎にフラグの数を変える
    select
        * except(text_flg),
        case 
            when row_number = 1 and text_flg >= 3 then 1 
            when row_number != 1 and text_flg >= 1 then 1
        end as header,
    from
        text_flg_add
),
running_header_add as(--joinのためrunning
    select
        *,
        sum(header) over(partition by date,description,stock_code order by row_number) as running_header,
    from
        header_add
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
        running_header_add
    where
        running_header >= 1
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
        row_number() over(partition by date,description,stock_code,running_header order by row_number) as new_row_number,
        row_number() over(partition by date,description,stock_code,running_header order by line_ad_period desc,line_quarter desc) as line_row_number --1なら年度が新しい方の行
    from
        ad_confirm
),
header_tb as (
    select * from new_row_number_add where header = 1 
),
unit_tb as(
    select * from new_row_number_add where new_row_number = 2
),
value_tb as(--単位が%の列はnullにする
    select
        t2.* except(col_1,col_2,col_3,col_4,col_5,col_6,col_7,col_8,col_9,col_10,col_11,col_12,col_13,col_14,col_15,col_16),
        case when nullif(t2.col_1,'%') is null then null else t1.col_1 end as col_1,
        case when nullif(t2.col_2,'%') is null then null else t1.col_2 end as col_2,
        case when nullif(t2.col_3,'%') is null then null else t1.col_3 end as col_3,
        case when nullif(t2.col_4,'%') is null then null else t1.col_4 end as col_4,
        case when nullif(t2.col_5,'%') is null then null else t1.col_5 end as col_5,
        case when nullif(t2.col_6,'%') is null then null else t1.col_6 end as col_6,
        case when nullif(t2.col_7,'%') is null then null else t1.col_7 end as col_7,
        case when nullif(t2.col_8,'%') is null then null else t1.col_8 end as col_8,
        case when nullif(t2.col_9,'%') is null then null else t1.col_9 end as col_9,
        case when nullif(t2.col_10,'%') is null then null else t1.col_10 end as col_10,
        case when nullif(t2.col_11,'%') is null then null else t1.col_11 end as col_11,
        case when nullif(t2.col_12,'%') is null then null else t1.col_12 end as col_12,
        case when nullif(t2.col_13,'%') is null then null else t1.col_13 end as col_13,
        case when nullif(t2.col_14,'%') is null then null else t1.col_14 end as col_14,
        case when nullif(t2.col_15,'%') is null then null else t1.col_15 end as col_15,
        case when nullif(t2.col_16,'%') is null then null else t1.col_16 end as col_16,
    from
        new_row_number_add as t1
    inner join
        unit_tb as t2
        on t1.date = t2.date and t1.description = t2.description and t1.stock_code = t2.stock_code and t1.running_header = t2.running_header 
    where
        t1.line_row_number = 1
),
value_tb_col_move as(--null列を除外して値の列のみで左寄せする
    select
        * except(col_1,col_2,col_3,col_4,col_5,col_6,col_7,col_8,col_9,col_10,col_11,col_12,col_13,col_14,col_15,col_16,vals),
        vals[safe_offset(0)].col as col_1, 
        vals[safe_offset(1)].col as col_2, 
        vals[safe_offset(2)].col as col_3, 
        vals[safe_offset(3)].col as col_4, 
        vals[safe_offset(4)].col as col_5, 
        vals[safe_offset(5)].col as col_6, 
        vals[safe_offset(6)].col as col_7, 
        vals[safe_offset(7)].col as col_8, 
        vals[safe_offset(8)].col as col_9, 
        vals[safe_offset(9)].col as col_10, 
        vals[safe_offset(10)].col as col_11, 
        vals[safe_offset(11)].col as col_12, 
        vals[safe_offset(12)].col as col_13, 
        vals[safe_offset(13)].col as col_14, 
        vals[safe_offset(14)].col as col_15, 
        vals[safe_offset(15)].col as col_16, 
    from(
        select 
            *,
            array(
                select as struct col
                from unnest([struct(1 as sort,col_1 as col),struct(2 as sort,col_2 as col),
                             struct(3 as sort,col_3 as col),struct(4 as sort,col_4 as col),struct(5 as sort,col_5 as col),
                             struct(6 as sort,col_6 as col),struct(7 as sort,col_7 as col),struct(8 as sort,col_8 as col)
                             ,struct(9 as sort,col_9 as col),struct(10 as sort,col_10 as col),struct(11 as sort,col_11 as col),
                             struct(12 as sort,col_12 as col),struct(13 as sort,col_13 as col),struct(14 as sort,col_14 as col),
                             struct(15 as sort,col_15 as col),struct(16 as sort,col_16 as col)
                             ])
                where col is not null 
                order by sort             
    ) as vals
    from
        value_tb
    )
),
first_header as(
    select
        t2.* except(col_1,col_2,col_3,col_4,col_5,col_6,col_7,col_8,col_9,col_10,col_11,col_12,col_13,col_14,col_15,col_16),
        t1.col_1 as label_1,
        t2.col_1 as col_1,
        t1.col_2 as label_2,
        t2.col_2 as col_2,
        t1.col_3 as label_3,
        t2.col_3 as col_3,
        t1.col_4 as label_4,
        t2.col_4 as col_4,
        t1.col_5 as label_5,
        t2.col_5 as col_5,
        t1.col_6 as label_6,
        t2.col_6 as col_6,
        t1.col_7 as label_7,
        t2.col_7 as col_7,
        t1.col_8 as label_8,
        t2.col_8 as col_8,
        t1.col_9 as label_9,
        t2.col_9 as col_9,
        t1.col_10 as label_10,
        t2.col_10 as col_10,
        t1.col_11 as label_11,
        t2.col_11 as col_11,
        t1.col_12 as label_12,
        t2.col_12 as col_12,
        t1.col_13 as label_13,
        t2.col_13 as col_13,
        t1.col_14 as label_14,
        t2.col_14 as col_14,
        t1.col_15 as label_15,
        t2.col_15 as col_15,
        t1.col_16 as label_16,
        t2.col_16 as col_16,
    from
        header_tb as t1
    left join
        value_tb_col_move as t2
        on t1.date = t2.date and t1.description = t2.description and t1.stock_code = t2.stock_code 
    where
        t1.running_header = 1 and t2.running_header = 1 
),
second_header as(
    select
        t2.* except(col_1,col_2,col_3,col_4,col_5,col_6,col_7,col_8,col_9,col_10,col_11,col_12,col_13,col_14,col_15,col_16),
        t1.col_1 as label_17,
        t2.col_1 as col_17,
        t1.col_2 as label_18,
        t2.col_2 as col_18,
        t1.col_3 as label_19,
        t2.col_3 as col_19,
        t1.col_4 as label_20,
        t2.col_4 as col_20,
        t1.col_5 as label_21,
        t2.col_5 as col_21,
        t1.col_6 as label_22,
        t2.col_6 as col_22,
        t1.col_7 as label_23,
        t2.col_7 as col_23,
        t1.col_8 as label_24,
        t2.col_8 as col_24,
        t1.col_9 as label_25,
        t2.col_9 as col_25,
        t1.col_10 as label_26,
        t2.col_10 as col_26,
        t1.col_11 as label_27,
        t2.col_11 as col_27,
        t1.col_12 as label_28,
        t2.col_12 as col_28,
        t1.col_13 as label_29,
        t2.col_13 as col_29,
        t1.col_14 as label_30,
        t2.col_14 as col_30,
        t1.col_15 as label_31,
        t2.col_15 as col_31,
        t1.col_16 as label_32,
        t2.col_16 as col_32,
    from
        header_tb as t1
    left join
        value_tb_col_move as t2
        on t1.date = t2.date and t1.description = t2.description and t1.stock_code = t2.stock_code 
    where
        t1.running_header = 2 and t2.running_header = 2
),
joint_tb as(
    select
        t1.* except(col_0,row_number,new_row_number,header,running_header,period_type,line_period_type,line_quarter,period,line_period,period_ad,line_period_ad,line_ad_period,line_row_number),
        t2.* except(date,description,stock_code,col_0,row_number,new_row_number,header,running_header,period_type,line_period_type,line_quarter,period,line_period,period_ad,line_period_ad,line_ad_period,line_row_number)
    from
        first_header as t1
    left join
        second_header as t2
        on t1.date = t2.date and t1.description = t2.description and t1.stock_code = t2.stock_code and t1.new_row_number = t2.new_row_number
),
joint_col_move as(
    select
        * except(vals),
        vals[safe_offset(0)].label as label_1, 
        vals[safe_offset(0)].col as col_1, 
        vals[safe_offset(1)].label as label_2, 
        vals[safe_offset(1)].col as col_2, 
        vals[safe_offset(2)].label as label_3, 
        vals[safe_offset(2)].col as col_3, 
        vals[safe_offset(3)].label as label_4, 
        vals[safe_offset(3)].col as col_4, 
        vals[safe_offset(4)].label as label_5, 
        vals[safe_offset(4)].col as col_5, 
        vals[safe_offset(5)].label as label_6, 
        vals[safe_offset(5)].col as col_6, 
        vals[safe_offset(6)].label as label_7, 
        vals[safe_offset(6)].col as col_7, 
        vals[safe_offset(7)].label as label_8, 
        vals[safe_offset(7)].col as col_8, 
        vals[safe_offset(8)].label as label_9, 
        vals[safe_offset(8)].col as col_9,
        vals[safe_offset(9)].label as label_10, 
        vals[safe_offset(9)].col as col_10, 
        vals[safe_offset(10)].label as label_11, 
        vals[safe_offset(10)].col as col_11, 
        vals[safe_offset(11)].label as label_12, 
        vals[safe_offset(11)].col as col_12, 
        vals[safe_offset(12)].label as label_13, 
        vals[safe_offset(12)].col as col_13, 
        vals[safe_offset(13)].label as label_14, 
        vals[safe_offset(13)].col as col_14, 
        vals[safe_offset(14)].label as label_15, 
        vals[safe_offset(14)].col as col_15, 
        vals[safe_offset(15)].label as label_16, 
        vals[safe_offset(15)].col as col_16, 
    from(
        select 
            date,description,stock_code,
            array(
                select as struct col,label
                from unnest([
                    struct(1 as sort,col_1 as col,label_1 as label),struct(2 as sort,col_2 as col,label_2 as label),struct(3 as sort,col_3 as col,label_3 as label),
                    struct(4 as sort,col_4 as col,label_4 as label),struct(5 as sort,col_5 as col,label_5 as label),struct(6 as sort,col_6 as col,label_6 as label),
                    struct(7 as sort,col_7 as col,label_7 as label),struct(8 as sort,col_8 as col,label_8 as label),struct(9 as sort,col_9 as col,label_9 as label),
                    struct(10 as sort,col_10 as col,label_10 as label),struct(11 as sort,col_11 as col,label_11 as label),struct(12 as sort,col_12 as col,label_12 as label),
                    struct(13 as sort,col_13 as col,label_13 as label),struct(14 as sort,col_14 as col,label_14 as label),struct(15 as sort,col_15 as col,label_15 as label),
                    struct(16 as sort,col_16 as col,label_16 as label),struct(17 as sort,col_17 as col,label_17 as label),struct(18 as sort,col_18 as col,label_18 as label),
                    struct(19 as sort,col_19 as col,label_19 as label),struct(20 as sort,col_20 as col,label_20 as label),struct(21 as sort,col_21 as col,label_21 as label),
                    struct(22 as sort,col_22 as col,label_22 as label),struct(23 as sort,col_23 as col,label_23 as label),struct(24 as sort,col_24 as col,label_24 as label),
                    struct(25 as sort,col_25 as col,label_25 as label),struct(26 as sort,col_26 as col,label_26 as label),struct(27 as sort,col_27 as col,label_27 as label),
                    struct(28 as sort,col_28 as col,label_28 as label),struct(29 as sort,col_29 as col,label_29 as label),struct(30 as sort,col_30 as col,label_30 as label),
                    struct(31 as sort,col_31 as col,label_31 as label),struct(32 as sort,col_32 as col,label_32 as label)
                ])
                where label is not null 
                order by sort             
    ) as vals
    from
        joint_tb
    )
),
confirm_tb as(--デバッグはここで行う、各labelに格納されているテキストで抜いているものが何か分かるようにしてある
    select
        *,
        case
            when label_2 in ('売上高','純営業収益','営業収益','売上収益','保険収益') then label_2 --基本的にはcol_1が売上高
            when label_2 = '経常利益' and label_1 = '経常収益' then label_1 --銀行は経常収益が売上,営業利益は記載がない
            else label_1
        end as earnings_label,
        case
            when label_2 in ('売上高','純営業収益','営業収益','売上収益','保険収益') then col_2
            when label_2 = '経常利益' and label_1 = '経常収益' then col_1
            else col_1
        end as earnings,
        case
            when label_2 in('営業利益','営業活動に係る利益','保険サービス損益') then label_2 --とにかくずばりその言葉があるならそのカラム
            when label_3 in('営業利益','営業活動に係る利益','保険サービス損益') then label_3
            when label_4 in('営業利益','営業活動に係る利益','保険サービス損益') then label_4
            when label_8 in('営業利益','営業活動に係る利益','保険サービス損益') then label_8 --参天製薬はコア＠＠を先に表示している
            when regexp_contains(label_2,r'営業利益|営業損失') then label_2
            when regexp_contains(label_3,r'営業利益|営業損失') then label_3
            when regexp_contains(label_4,r'営業利益|営業損失') then label_4
            when regexp_contains(label_2,r'EBITDA') and regexp_contains(label_3,r'事業利益') then label_3
            when label_2 = '経常利益' and label_1 = '経常収益' then null --銀行は経常収益が売上,営業利益は記載がない
            when label_2 in ('税引前利益','経常利益','経常損益') then null --2列目に経常利益が来ている=営業利益がない
            when label_2 in ('財務・法人所得税前利益') then null --JALのみ、label_3には経常利益が来ているが財務・法人所得税前利益は営業利益ではない        
            when regexp_contains(label_2,r'事業利益') then null    
            else label_2
        end as operating_income_label,
        case
            when label_2 in('営業利益','営業活動に係る利益','保険サービス損益') then col_2 --とにかくずばりその言葉があるならそのカラム
            when label_3 in('営業利益','営業活動に係る利益','保険サービス損益') then col_3
            when label_4 in('営業利益','営業活動に係る利益','保険サービス損益') then col_4
            when label_8 in('営業利益','営業活動に係る利益','保険サービス損益') then col_8 --参天製薬はコア＠＠を先に表示している
            when regexp_contains(label_2,r'営業利益|営業損失') then col_2
            when regexp_contains(label_3,r'営業利益|営業損失') then col_3
            when regexp_contains(label_4,r'営業利益|営業損失') then col_4
            when regexp_contains(label_2,r'EBITDA') and regexp_contains(label_3,r'事業利益') then col_3
            when label_2 = '経常利益' and label_1 = '経常収益' then null --銀行は経常収益が売上,営業利益は記載がない
            when label_2 in ('税引前利益','経常利益','経常損益') then null --2列目に経常利益が来ている=営業利益がない
            when label_2 in ('財務・法人所得税前利益') then null --JALのみ、label_3には経常利益が来ているが財務・法人所得税前利益は営業利益ではない        
            when regexp_contains(label_2,r'事業利益') then null    
            else col_2
        end as operating_income,
        case
            when label_2 in('経常利益','経常損失') then label_2
            when label_3 in('経常利益','経常損失') then label_3
            when label_4 in('経常利益','経常損失') then label_4
            when label_5 in('経常利益','経常損失') then label_5
            when regexp_contains(label_2,r'経常利益|経常損失|税引前利益|税引前中間利益|税引前利益|税引前当期純利益|税引前四半期利益|税引前四半期純利益|税引前当期利益|税金等調整前当期純利益') and label_2 not like '%利益率%'then label_2
            when regexp_contains(label_3,r'経常利益|経常損失|税引前利益|税引前中間利益|税引前利益|税引前当期純利益|税引前四半期利益|税引前四半期純利益|税引前当期利益|税金等調整前当期純利益') and label_3 not like '%利益率%'then label_3
            when regexp_contains(label_4,r'経常利益|経常損失|税引前利益|税引前中間利益|税引前利益|税引前当期純利益|税引前四半期利益|税引前四半期純利益|税引前当期利益|税金等調整前当期純利益') and label_4 not like '%利益率%'then label_4
            when regexp_contains(label_5,r'経常利益|経常損失|税引前利益|税引前中間利益|税引前利益|税引前当期純利益|税引前四半期利益|税引前四半期純利益|税引前当期利益|税金等調整前当期純利益') and label_5 not like '%利益率%'then label_5
            when regexp_contains(label_9,r'経常利益|経常損失|税引前利益|税引前中間利益|税引前利益|税引前当期純利益|税引前四半期利益|税引前四半期純利益|税引前当期利益|税金等調整前当期純利益') and label_9 not like '%利益率%'then label_9 --参天製薬
            when label_2 = '営業利益' and label_3 in('四半期利益','当期利益','中間利益') then null
            when label_3 = '営業利益' and label_4 in('四半期利益','当期利益','中間利益') then null
            when label_2 like '%調整後営業利益%' and label_3 = '営業利益' then label_3 --ブリジストンのみ調整後営業利益が営業利益で営業利益が経常利益
        end as ordinaly_profit_label,
        case
            when label_2 in('経常利益','経常損失') then col_2
            when label_3 in('経常利益','経常損失') then col_3
            when label_4 in('経常利益','経常損失') then col_4
            when label_5 in('経常利益','経常損失') then col_5
            when regexp_contains(label_2,r'経常利益|経常損失|税引前利益|税引前中間利益|税引前利益|税引前当期純利益|税引前四半期利益|税引前四半期純利益|税引前当期利益|税金等調整前当期純利益') and label_2 not like '%利益率%'then col_2
            when regexp_contains(label_3,r'経常利益|経常損失|税引前利益|税引前中間利益|税引前利益|税引前当期純利益|税引前四半期利益|税引前四半期純利益|税引前当期利益|税金等調整前当期純利益') and label_3 not like '%利益率%'then col_3
            when regexp_contains(label_4,r'経常利益|経常損失|税引前利益|税引前中間利益|税引前利益|税引前当期純利益|税引前四半期利益|税引前四半期純利益|税引前当期利益|税金等調整前当期純利益') and label_4 not like '%利益率%'then col_4
            when regexp_contains(label_5,r'経常利益|経常損失|税引前利益|税引前中間利益|税引前利益|税引前当期純利益|税引前四半期利益|税引前四半期純利益|税引前当期利益|税金等調整前当期純利益') and label_5 not like '%利益率%'then col_5
            when regexp_contains(label_9,r'経常利益|経常損失|税引前利益|税引前中間利益|税引前利益|税引前当期純利益|税引前四半期利益|税引前四半期純利益|税引前当期利益|税金等調整前当期純利益') and label_9 not like '%利益率%'then col_9 --参天製薬
            when label_2 = '営業利益' and label_3 in('四半期利益','当期利益','中間利益') then null
            when label_3 = '営業利益' and label_4 in('四半期利益','当期利益','中間利益') then null
            when label_2 like '%調整後営業利益%' and label_3 = '営業利益' then col_3 --ブリジストンのみ調整後営業利益が営業利益で営業利益が経常利益
        end as ordinaly_profit,   
        case
            when regexp_contains(label_3,r'親会社株主に帰属|親会社の所有に帰属|親会社の所有者に帰属|当社に帰属する|当社株主に帰属') and regexp_contains(label_3,r'利益|損失|損益') and not regexp_contains(label_3,r'コア') then label_3
            when regexp_contains(label_4,r'親会社株主に帰属|親会社の所有に帰属|親会社の所有者に帰属|当社に帰属する|当社株主に帰属') and regexp_contains(label_4,r'利益|損失|損益') and not regexp_contains(label_4,r'コア') then label_4
            when regexp_contains(label_5,r'親会社株主に帰属|親会社の所有に帰属|親会社の所有者に帰属|当社に帰属する|当社株主に帰属') and regexp_contains(label_5,r'利益|損失|損益') and not regexp_contains(label_5,r'コア') then label_5
            when regexp_contains(label_6,r'親会社株主に帰属|親会社の所有に帰属|親会社の所有者に帰属|当社に帰属する|当社株主に帰属') and regexp_contains(label_6,r'利益|損失|損益') and not regexp_contains(label_6,r'コア') then label_6
            when regexp_contains(label_7,r'親会社株主に帰属|親会社の所有に帰属|親会社の所有者に帰属|当社に帰属する|当社株主に帰属') and regexp_contains(label_7,r'利益|損失|損益') and not regexp_contains(label_7,r'コア') then label_7
            when regexp_contains(label_10,r'親会社株主に帰属|親会社の所有に帰属|親会社の所有者に帰属する|当社に帰属する|当社株主に帰属する') and regexp_contains(label_10,r'利益|損失|損益') and not regexp_contains(label_10,r'コア') then label_10
            when regexp_contains(label_11,r'親会社株主に帰属|親会社の所有に帰属|親会社の所有者に帰属する|当社に帰属する|当社株主に帰属する') and regexp_contains(label_11,r'利益|損失|損益') and not regexp_contains(label_11,r'コア') then label_11
            when regexp_contains(label_3,r'四半期利益|四半期純利益|当期利益|当期純利益|中間利益|中間純利益') and not regexp_contains(label_3,r'税引前') then label_3
            when regexp_contains(label_4,r'四半期利益|四半期純利益|当期利益|当期純利益|中間利益|中間純利益') and not regexp_contains(label_4,r'税引前') then label_4
            when regexp_contains(label_5,r'四半期利益|四半期純利益|当期利益|当期純利益|中間利益|中間純利益') and not regexp_contains(label_5,r'税引前') then label_5
            when regexp_contains(label_6,r'四半期利益|四半期純利益|当期利益|当期純利益|中間利益|中間純利益') and not regexp_contains(label_6,r'税引前') then label_6
            when regexp_contains(label_7,r'四半期利益|四半期純利益|当期利益|当期純利益|中間利益|中間純利益') and not regexp_contains(label_7,r'税引前') then label_7
            when regexp_contains(label_10,r'四半期利益|四半期純利益|当期利益|当期純利益|中間利益|中間純利益') and not regexp_contains(label_10,r'税引前') then label_10 --参天製薬
            when regexp_contains(label_11,r'四半期利益|四半期純利益|当期利益|当期純利益|中間利益|中間純利益') and not regexp_contains(label_11,r'税引前') then label_11 --参天製薬
        end as net_income_label,
        case
            when regexp_contains(label_3,r'親会社株主に帰属|親会社の所有に帰属|親会社の所有者に帰属|当社に帰属する|当社株主に帰属') and regexp_contains(label_3,r'利益|損失|損益') and not regexp_contains(label_3,r'コア') then col_3
            when regexp_contains(label_4,r'親会社株主に帰属|親会社の所有に帰属|親会社の所有者に帰属|当社に帰属する|当社株主に帰属') and regexp_contains(label_4,r'利益|損失|損益') and not regexp_contains(label_4,r'コア') then col_4
            when regexp_contains(label_5,r'親会社株主に帰属|親会社の所有に帰属|親会社の所有者に帰属|当社に帰属する|当社株主に帰属') and regexp_contains(label_5,r'利益|損失|損益') and not regexp_contains(label_5,r'コア') then col_5
            when regexp_contains(label_6,r'親会社株主に帰属|親会社の所有に帰属|親会社の所有者に帰属|当社に帰属する|当社株主に帰属') and regexp_contains(label_6,r'利益|損失|損益') and not regexp_contains(label_6,r'コア') then col_6
            when regexp_contains(label_7,r'親会社株主に帰属|親会社の所有に帰属|親会社の所有者に帰属|当社に帰属する|当社株主に帰属') and regexp_contains(label_7,r'利益|損失|損益') and not regexp_contains(label_7,r'コア') then col_7
            when regexp_contains(label_10,r'親会社株主に帰属|親会社の所有に帰属|親会社の所有者に帰属する|当社に帰属する|当社株主に帰属する') and regexp_contains(label_10,r'利益|損失|損益') and not regexp_contains(label_10,r'コア') then col_10
            when regexp_contains(label_11,r'親会社株主に帰属|親会社の所有に帰属|親会社の所有者に帰属する|当社に帰属する|当社株主に帰属する') and regexp_contains(label_11,r'利益|損失|損益') and not regexp_contains(label_11,r'コア') then col_11
            when regexp_contains(label_3,r'四半期利益|四半期純利益|当期利益|当期純利益|中間利益|中間純利益') and not regexp_contains(label_3,r'税引前') then col_3
            when regexp_contains(label_4,r'四半期利益|四半期純利益|当期利益|当期純利益|中間利益|中間純利益') and not regexp_contains(label_4,r'税引前') then col_4
            when regexp_contains(label_5,r'四半期利益|四半期純利益|当期利益|当期純利益|中間利益|中間純利益') and not regexp_contains(label_5,r'税引前') then col_5
            when regexp_contains(label_6,r'四半期利益|四半期純利益|当期利益|当期純利益|中間利益|中間純利益') and not regexp_contains(label_6,r'税引前') then col_6
            when regexp_contains(label_7,r'四半期利益|四半期純利益|当期利益|当期純利益|中間利益|中間純利益') and not regexp_contains(label_7,r'税引前') then col_7
            when regexp_contains(label_10,r'四半期利益|四半期純利益|当期利益|当期純利益|中間利益|中間純利益') and not regexp_contains(label_10,r'税引前') then col_10 --参天製薬
            when regexp_contains(label_11,r'四半期利益|四半期純利益|当期利益|当期純利益|中間利益|中間純利益') and not regexp_contains(label_11,r'税引前') then col_11 --参天製薬
        end as net_income,
    from
        joint_col_move
),
cast_tb as(
    select
        date,
        description,
        stock_code,
--        earnings as earnings_str,
--        operating_income as operating_income_str,
--        ordinaly_profit as ordinaly_profit_str,
--        net_income as net_income_str,
        cast(replace(replace(earnings,'(※)',''),'△','-') as float64) as earnings,
        cast(replace(replace(operating_income,'(※)',''),'△','-') as float64) as operating_income,
        cast(replace(replace(ordinaly_profit,'(※)',''),'△','-') as float64) as ordinaly_profit,
        cast(replace(replace(net_income,'(※)',''),'△','-') as float64) as net_income
    from
        confirm_tb

)
select * from cast_tb