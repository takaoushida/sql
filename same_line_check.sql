with
errors as(
    select
        stock_code,
        period_month,
        period,
        quarter,
        count(*) cnt
    from
        jpx.refine_securities_report_master
    where
        refine_flg is null
    group by 1,2,3,4
    having cnt > 1 
),
stock_codes as(--プロマーケットなどのデータも入っているので'グロース','スタンダード','プライム'に限定
    select
        distinct
        code as stock_code
    from
        `stock_data_mst.stock_data_mst_tokyo_01`
    union distinct
    select
        stock_code
    from
        `stock_data_mst.delisting_*`
    where
        market_category in('グロース','スタンダード','プライム')
),
stock_data as(
    select
        t1.*
    from
        temp_folder.all_summary_title as t1
    inner join
        stock_codes as t2
        on t1.stock_code = t2.stock_code
),
space_cut as(--漢字の数値を置換
    select
        distinct
        stock_code,
        release_date,
        xbrl,
        replace(replace(replace(replace(replace(title,' ',''),'一','1'),'二','2'),'三','3'),'元年','1年') as title,
        replace(replace(replace(replace(replace(inpage_title,' ',''),'一','1'),'二','2'),'三','3'),'元年','1年') as inpage_title,
        case when regexp_contains(title,r'訂正|変更|修正|追加|再登録|差替|レビ') then 1 end as refine_flg
    from
        stock_data
    where
        xbrl is not null --xbrlが存在する行は確実に決算短信
        or (
            not regexp_contains(title,r'訂正|変更|再登録|追加|超えること|差替|キャッシュ・フロー計算書|DATABOOK|Q&A|PDF|について|お知らせ|サマリー|高い関心|データ集|推移表|~|FAQ|記者会見|報告会|取り組み')--xbrlがなく、これらのキーワードを含んでいないなら決算短信
            and title not like '%報告' and title not like '%報告書' --報告で終わるタイトルは決算短信ではない
            and title not like '%決算' --決算で終わるタイトルは決算短信ではない ※[日本基準]などついているはず
        ) 
),
period_add as(--西暦,決算月,quarterを取得
    select
        *,
        --titleの各値
        CASE
            WHEN REGEXP_CONTAINS(title, r'([0-9]{4})年') THEN CAST(REGEXP_EXTRACT(title, r'([0-9]{4})年') AS INT64)            
            WHEN REGEXP_CONTAINS(title, r'令和([0-9]{1,2})年') THEN 2018 + CAST(REGEXP_EXTRACT(title, r'令和([0-9]{1,2})年') AS INT64)
            WHEN REGEXP_CONTAINS(title, r'平成([0-9]{1,2})年') THEN 1988 + CAST(REGEXP_EXTRACT(title, r'平成([0-9]{1,2})年') AS INT64)
            WHEN REGEXP_CONTAINS(title, r'([0-9]{4})n年') THEN CAST(REGEXP_EXTRACT(title, r'([0-9]{4})n年') AS INT64)
            WHEN REGEXP_CONTAINS(title, r'([0-9]{4})期') THEN CAST(REGEXP_EXTRACT(title, r'([0-9]{4})期') AS INT64)
            WHEN REGEXP_CONTAINS(title, r'([0-9]{4})月') THEN CAST(REGEXP_EXTRACT(title, r'([0-9]{4})月') AS INT64)
            WHEN REGEXP_CONTAINS(title, r'平成([0-9]{1,2})月') THEN 1988 + CAST(REGEXP_EXTRACT(title, r'平成([0-9]{1,2})月') AS INT64)
            WHEN REGEXP_CONTAINS(title, r'平成([0-9]{1,2})期') THEN 1988 + CAST(REGEXP_EXTRACT(title, r'平成([0-9]{1,2})期') AS INT64)
            WHEN REGEXP_CONTAINS(title, r'成([0-9]{1,2})年') THEN 1988 + CAST(REGEXP_EXTRACT(title, r'成([0-9]{1,2})年') AS INT64)
            WHEN REGEXP_CONTAINS(title, r'平([0-9]{1,2})年') THEN 1988 + CAST(REGEXP_EXTRACT(title, r'平([0-9]{1,2})年') AS INT64)
            WHEN REGEXP_CONTAINS(title, r'([0-9]{2})年') THEN 2000 + CAST(REGEXP_EXTRACT(title, r'([0-9]{2})年') AS INT64)
        END AS period,
        case
            when REGEXP_CONTAINS(title, r'年([0-9]{1,2})月期') then  CAST(REGEXP_extract(title, r'年([0-9]{1,2})月期') AS INT64)
            when REGEXP_CONTAINS(title, r'年度([0-9]{1,2})月期') then  CAST(REGEXP_extract(title, r'年度([0-9]{1,2})月期') AS INT64)
            when REGEXP_CONTAINS(title, r'月([0-9]{1,2})月期') then  CAST(REGEXP_extract(title, r'月([0-9]{1,2})月期') AS INT64)
            when REGEXP_CONTAINS(title, r'年([0-9]{1,2})年期') then  CAST(REGEXP_extract(title, r'年([0-9]{1,2})年期') AS INT64)
            when REGEXP_CONTAINS(title, r'年\)([0-9]{1,2})月期') then  CAST(REGEXP_extract(title, r'年\)([0-9]{1,2})月期') AS INT64)
        end as period_month,
        CASE
            WHEN REGEXP_CONTAINS(title, r'第([0-9]{1})四半期') THEN CAST(REGEXP_EXTRACT(title, r'第([0-9]{1})四半期') AS INT64)
            WHEN REGEXP_CONTAINS(title, r'([0-9]{1})Q') THEN CAST(REGEXP_EXTRACT(title, r'([0-9]{1})Q') AS INT64)
            ELSE 4
        END AS quarter,
        --inpage_titleの各値
        CASE
            WHEN REGEXP_CONTAINS(inpage_title, r'([0-9]{4})年') THEN CAST(REGEXP_EXTRACT(inpage_title, r'([0-9]{4})年') AS INT64)            
            WHEN REGEXP_CONTAINS(inpage_title, r'令和([0-9]{1,2})年') THEN 2018 + CAST(REGEXP_EXTRACT(inpage_title, r'令和([0-9]{1,2})年') AS INT64)
            WHEN REGEXP_CONTAINS(inpage_title, r'平成([0-9]{1,2})年') THEN 1988 + CAST(REGEXP_EXTRACT(inpage_title, r'平成([0-9]{1,2})年') AS INT64)
            WHEN REGEXP_CONTAINS(inpage_title, r'([0-9]{4})n年') THEN CAST(REGEXP_EXTRACT(inpage_title, r'([0-9]{4})n年') AS INT64)
            WHEN REGEXP_CONTAINS(inpage_title, r'([0-9]{4})期') THEN CAST(REGEXP_EXTRACT(inpage_title, r'([0-9]{4})期') AS INT64)
            WHEN REGEXP_CONTAINS(inpage_title, r'([0-9]{4})月') THEN CAST(REGEXP_EXTRACT(inpage_title, r'([0-9]{4})月') AS INT64)
            WHEN REGEXP_CONTAINS(inpage_title, r'平成([0-9]{1,2})月') THEN 1988 + CAST(REGEXP_EXTRACT(inpage_title, r'平成([0-9]{1,2})月') AS INT64)
            WHEN REGEXP_CONTAINS(inpage_title, r'平成([0-9]{1,2})期') THEN 1988 + CAST(REGEXP_EXTRACT(inpage_title, r'平成([0-9]{1,2})期') AS INT64)
            WHEN REGEXP_CONTAINS(inpage_title, r'成([0-9]{1,2})年') THEN 1988 + CAST(REGEXP_EXTRACT(inpage_title, r'成([0-9]{1,2})年') AS INT64) --※「平」が抜けてるケース
            WHEN REGEXP_CONTAINS(inpage_title, r'平([0-9]{1,2})年') THEN 1988 + CAST(REGEXP_EXTRACT(inpage_title, r'平([0-9]{1,2})年') AS INT64) --※「成」が抜けてるケース
            WHEN REGEXP_CONTAINS(inpage_title, r'([0-9]{2})年') THEN 2000 + CAST(REGEXP_EXTRACT(inpage_title, r'([0-9]{2})年') AS INT64)
            --else cast(format_date('%Y',release_date) as int64)
        END AS inpage_period,
        case
            when REGEXP_CONTAINS(inpage_title, r'年([0-9]{1,2})月期') then  CAST(REGEXP_extract(inpage_title, r'年([0-9]{1,2})月期') AS INT64)
            when REGEXP_CONTAINS(inpage_title, r'年度([0-9]{1,2})月期') then  CAST(REGEXP_extract(inpage_title, r'年度([0-9]{1,2})月期') AS INT64)
            when REGEXP_CONTAINS(inpage_title, r'月([0-9]{1,2})月期') then  CAST(REGEXP_extract(inpage_title, r'月([0-9]{1,2})月期') AS INT64)
            when REGEXP_CONTAINS(inpage_title, r'年([0-9]{1,2})年期') then  CAST(REGEXP_extract(inpage_title, r'年([0-9]{1,2})年期') AS INT64)
            when REGEXP_CONTAINS(inpage_title, r'年\)([0-9]{1,2})月期') then  CAST(REGEXP_extract(inpage_title, r'年\)([0-9]{1,2})月期') AS INT64)
        end as inpage_period_month,
        CASE
            WHEN REGEXP_CONTAINS(inpage_title, r'第([0-9]{1})四半期') THEN CAST(REGEXP_EXTRACT(inpage_title, r'第([0-9]{1})四半期') AS INT64)
            WHEN REGEXP_CONTAINS(inpage_title, r'([0-9]{1})Q') THEN CAST(REGEXP_EXTRACT(inpage_title, r'([0-9]{1})Q') AS INT64)
        END AS inpage_quarter,    
    from
        space_cut
),
select_tb as(--正しい方を取得
    select
        stock_code,
        release_date,
        xbrl,
        title,
        inpage_title,
        greatest(ifnull(period,0),ifnull(inpage_period,0)) as period,--前回のコピペで1年前になってるケースがある,その場合inpageはそうなってない ※例外あり
        ifnull(coalesce(inpage_period_month,period_month),0) as period_month,--どちらもnullというのはあり得る
        ifnull(coalesce(inpage_quarter,quarter),4) as quarter,--inpage_quarterを優先とする、どちらもnullなら4
        refine_flg
    from
        period_add
),
--決算月変更フラグを立てるためのサブクエリ
--年度またぎで変更するケースを検知
last_period_month_add as(--訂正じゃない行に前回のlast_periodを付与
    select
        *,
        lag(period_month,1) over(partition by stock_code order by release_date) as last_period_month
    from
        select_tb
    where
        refine_flg is null
),
change_flg_add as(--決算月に変更があったquarterにフラグを立てる
    select
        *,
        case when period_month != last_period_month then 1 end as change_flg --どちらかがnullだった場合はフラグが立たない。priod_monthはnullはない(ifnull済)、特に最初の行はlast_period_monthは確実にnull
    from
        last_period_month_add
),
change_flg_count_tb as(--そのまま結合だとそのquarterにしかフラグをつけれないのでperiodで立てるために集計
    select
        stock_code,
        period,
        sum(change_flg) as change_flg
    from
        change_flg_add    
    group by 1,2
),
--年度内で変更するケースを検知
year_period_months as(
    select
        stock_code,
        period,
        count(distinct period_month) as period_month_cnt
    from
        select_tb
    group by 1,2
),
refine_add_change_flg as(--periodごとにchange_flgを付与
    select
        t1.*,
        case
            when t2.change_flg is not null then t2.change_flg --年度またぎでフラグが立った場合
            when t3.period_month_cnt >= 2 then 1 --同年度内に複数の決算月がある
        end as change_flg
    from
        select_tb as t1
    left join
        change_flg_count_tb as t2
        on t1.stock_code = t2.stock_code and t1.period = t2.period
    left join
        year_period_months as t3
        on t1.stock_code = t3.stock_code and t1.period = t3.period
),
--ここでspreadsheetを参照し、手動修正を反映
spreadsheet_add as(
    select
        t1.* except(period,period_month,quarter,refine_flg,change_flg),
        coalesce(t2.period,t1.period) as period,
        coalesce(t2.period_month,t1.period_month) as period_month,
        coalesce(t2.quarter,t1.quarter) as quarter,
        coalesce(t2.refine_flg,t1.refine_flg) as refine_flg,
        coalesce(t2.change_flg,t1.change_flg) as change_flg,
        t2.omit_flg--spreadsheetでここが1と記載された行は無効行とする
    from
        refine_add_change_flg as t1
    left join
        spreadsheet_link.rename_sheet as t2
        on t1.stock_code = t2.stock_code and t1.release_date = t2.release_date and t1.title = t2.title
)
select
    t1.*,
    t2.cnt
from
    spreadsheet_add as t1
inner join
    errors as t2
    using(stock_code,period_month,period,quarter)
where 
    t1.refine_flg is null and t1.change_flg is null
order by 1,2,4,5,6
