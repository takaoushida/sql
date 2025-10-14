with
count_tb as(
    select
        stock_code,
        period,
        count(quarter) as q_cnt,
        max(quarter) as max_quarter,
        sum(change_flg) as change_flg
    from
        jpx.refine_securities_report_master
    where
        refine_flg is null
    group by 1,2
),
min_add as(
    select
        *,
        min(period) over(partition by stock_code) as min_period,
    from
        count_tb
),
stock_datas as(
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
errors as(--期数がおかしい銘柄の年度
    select
        t1.stock_code,
        t1.period,
        t1.q_cnt,
        t1.max_quarter
    from
        min_add as t1
    inner join
        stock_datas as t2
        on t1.stock_code = t2.stock_code
    where
        period != min_period and q_cnt != max_quarter and change_flg is null
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
        temp_folder.all_summary_title
    where
        xbrl is not null --xbrlが存在する行は確実に決算短信
        or (
            not regexp_contains(title,r'資料|訂正|変更|再登録|追加|超えること|差替|キャッシュ・フロー計算書|DATABOOK|Q&A|PDF|について|お知らせ|サマリー|高い関心|データ集|推移表|~|FAQ|記者会見|報告会|取り組み|説明|書き起こし')--xbrlがなく、これらのキーワードを含んでいないなら決算短信
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
        greatest(ifnull(period,0),ifnull(inpage_period,0)) as period,--前回のコピペで1年前になってるケースがある,その場合inpageはそうなってない ※例外あり
        ifnull(coalesce(inpage_period_month,period_month),0) as period_month,--どちらもnullというのはあり得る
        ifnull(coalesce(inpage_quarter,quarter),4) as quarter,--inpage_quarterを優先とする、どちらもnullなら4
        refine_flg
    from
        period_add
)
select
    t1.*,
    t2.q_cnt,
    t2.max_quarter
from
    select_tb as t1
inner join
    errors as t2
    on t1.stock_code = t2.stock_code and t1.period = t2.period
order by stock_code,release_date
