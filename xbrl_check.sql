with
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
            not regexp_contains(title,r'資料|訂正|変更|再登録|追加|超えること|差替|キャッシュ・フロー計算書|DATABOOK|Q&A|PDF|について|お知らせ|サマリー|高い関心|データ集|推移表|~|FAQ|記者会見|報告会|取り組み|説明|書き起こし')--xbrlがなく、これらのキーワードを含んでいないなら決算短信
            and title not like '%報告' and title not like '%報告書' --報告で終わるタイトルは決算短信ではない
            and title not like '%決算' --決算で終わるタイトルは決算短信ではない ※[日本基準]などついているはず
        )
)
select
    t1.*
from
    space_cut as t1
left join
    spreadsheet_link.xbrl_less_summary as t2
    on t1.stock_code = t2.stock_code and t1.release_date = t2.release_date and t1.title = t2.title
left join
    spreadsheet_link.rename_sheet as t3
    on t1.stock_code = t3.stock_code and t1.release_date = t3.release_date and t1.title = t3.title
where
    xbrl is null
    and t2.stock_code is null
    and t3.omit_flg is null
order by stock_code,release_date