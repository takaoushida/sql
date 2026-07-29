select
    t1.stock_code,
    t1.split_date,
    count(t1.release_date) over(partition by t1.stock_code,t1.split_date) as cnt
from
    `stock_data.stock_split_*` as t1
left join   
    `valid-responder-219005.spreadsheet_link.stock_split_rename_sheet` as t2
    on t1.stock_code = t2.stock_code and t1.release_date = t2.release_date and t1.pdf_url = t2.pdf_url
where   
    t2.omit_flg is null  
qualify cnt != 1
