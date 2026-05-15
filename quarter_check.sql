with
window_tb as(
    select  
        *,
        min(period) over(partition by stock_code) as min_period,
        max(quarter) over(partition by stock_code,period) as max_quarter,
        count(quarter) over(partition by stock_code,period) as quarter_cnt
    from   
        jpx.refine_securities_report_master
),
mst as(
    select
        distinct
        code as stock_code,
    from
        `stock_data_mst.stock_data_mst_tokyo_01`
    union distinct
    select
        stock_code
    from
        `stock_data_mst.delisting_20*`
    where   
        stock_code != 'None' and omit_flg is not null
    union distinct
    select
        stock_code
    from
        `stock_data_mst.delisting_reason*`
    where
        market_category in('グロース','スタンダード','プライム')   
)
select
    t1.* except(min_period,xbrl)
from    
    window_tb as t1
inner join
    mst as t2
    on t1.stock_code = t2.stock_code
left join
    spreadsheet_link.rename_sheet as t3
    on t1.stock_code = t3.stock_code and t1.release_date = t3.release_date and t1.title = t3.title
where 
    t1.period != t1.min_period and t1.quarter_cnt != t1.max_quarter
    and t1.change_flg is null and t3.reason is null
order by stock_code,release_date
