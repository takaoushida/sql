DECLARE suffix STRING;
DECLARE today STRING;
for tables in(
    select 
        cast(current_date('Asia/Tokyo') as string) as today,
        format_date('%Y%m%d',current_date('Asia/Tokyo')) as suffix
)

    do  
        execute immediate format(
            """
            create or replace table `looker_datamart.daily_feature_learning_%s` as
            select
                * except(row_number,last_row_number, dual_num)
            from
                looker_datamart.feature_learning
            where
                created_at = '%s'
            """, 
        tables.suffix,tables.today
        );
end for;
