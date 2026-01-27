DECLARE stock_code STRING;
DECLARE suffix STRING;


for tables in(
    with
    delisting_mst as(
        select
            stock_code,
            end_date
        from
            `stock_data_mst.delisting_20*`
    ),
    delisting_dataset as(
        select
            distinct
            replace(table_id,'delisting_','') as stock_code
        from    
            stock_data_delisting.__TABLES__
    ),
    dataless_delisting as(
        select  
            t1.*
        from    
            delisting_mst as t1
        left join
            delisting_dataset as t2
            on t1.stock_code = t2.stock_code
        where  
            t2.stock_code is null
    ),
    tokyo_01 as(
        select
            distinct
            replace(table_id,'tokyo_01_','') as suffix_date,


        from
            stock_data.__TABLES__
        where
            table_id like '%tokyo_01_%'
    )
    select
        t1.stock_code,
        max(t2.suffix_date) as suffix
    from
        dataless_delisting as t1
    inner join
        tokyo_01 as t2
        on t1.end_date >= parse_date('%Y%m%d',t2.suffix_date)
    group by 1
)
    do  
        execute immediate format(
            """
            create or replace table stock_data_delisting.delisting_%s
            partition by date as(
                select
                    *
                from
                    `stock_data.tokyo_01_%s`
                where
                    stock_code = '%s'
            )
            """,
        tables.stock_code,tables.suffix,tables.stock_code
        );
end for;







