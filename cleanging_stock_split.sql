create or replace table stock_data.cleanging_stock_split
partition by release_date as(
    with
    max_add as(
        select
            distinct
            * except(pdf_url),
            max(release_date) over(partition by stock_code,base_date) as max_release_date
        from
            `stock_data.stock_split_20250801`
    )
    select
        * except(max_release_date)
    from
        max_add
    where
        release_date = max_release_date
);
