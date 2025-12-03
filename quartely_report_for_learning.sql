create or replace table securities_report.quartely_report_for_learning
partition by release_date
cluster by stock_code as(
with
header_tb as(
    select
        *
    from
        temp_folder.all_summary
    where
        row_number = 1
),
after_tb as(
    select
        *
    from
        temp_folder.all_summary
    where
        row_number = 3
),
before_tb as(
    select
        *
    from
        temp_folder.all_summary
    where
        row_number = 4
),
summary_tb as(
    select
        t1.xbrl,
        case
            when t1.col_2 in ('売上高','売上収益') and t1.col_3 = '営業収益' then t1.col_2 --2897
            when regexp_contains(t1.col_3,r'売上高|売上収益|保険収益|営業収益') then t1.col_3
            else t1.col_2 
        end as earnings_title,--売上高
        case 
            when t1.col_3 = 'コア営業利益' and t1.col_9 = '営業利益' then t1.col_9
            when t1.col_3 in ('コア営業利益','事業利益') and t1.col_4 like '税引前%' then t1.col_3
            when t1.col_4 like '税引前%' and t1.col_3 like '事業利益%' then t1.col_3
            when t1.col_5 like '税引前%' and t1.col_4 like '事業利益%' then t1.col_4
            when t1.col_5 in('営業利益','営業損失','営業活動に係る利益') then t1.col_5
            when t1.col_4 in('営業利益','営業損失','営業活動に係る利益') or t1.col_4 in('収益') then t1.col_4
            when t1.col_3 in('営業利益','営業損失','営業活動に係る利益') then t1.col_3
            when t1.col_2 = '保険収益' or t1.col_3 = '保険収益'  then null --7157,9630(保険業)
            when t1.col_3 = '収益合計(金融費用控除後)' then null --8604(野村)
            when t1.col_3 = 'EBITDA※1' and t1.col_4 = '事業利益※2' then t1.col_4 --3491　※2023年以前
            when t1.col_3 like '調整後営業利益%' and t1.col_4 != '営業利益' then t1.col_3 
            when t1.col_3 in ('経常利益','経常損益') or t1.col_3 like '税引前%' then null
            when regexp_contains(t1.col_3,r'営業利益|営業損失|営業収益') then t1.col_3--Non-GAAP営業利益とか営業損失(-)とか
        end as operating_income_title,--営業利益
        case    
            when t1.col_4 = 'コア四半期利益' and t1.col_10 = '税引前四半期利益' then t1.col_10
            when t1.col_4 = 'コア当期利益' and t1.col_10 = '税引前当期利益' then t1.col_10
            when regexp_contains(t1.col_3,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_3 not like '%利益率' then t1.col_3
            when regexp_contains(t1.col_4,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_4 not like '%利益率' then t1.col_4
            when regexp_contains(t1.col_5,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_5 not like '%利益率' then t1.col_5
            when regexp_contains(t1.col_6,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_6 not like '%利益率' then t1.col_6
            when regexp_contains(t1.col_7,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_7 not like '%利益率' then t1.col_7
            when regexp_contains(t1.col_8,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_8 not like '%利益率' then t1.col_8
            when t1.col_3 = '営業利益' and t1.col_4 in ('中間利益','四半期利益','親会社の所有者に帰属する中間利益','当期利益') then t1.col_4
            when t1.col_4 ='営業利益' and t1.col_5 in ('中間利益','四半期利益','親会社の所有者に帰属する中間利益','当期利益') then t1.col_5
            when t1.col_5 = '親会社の所有者に帰属する中間利益' and t1.col_4 = '中間利益' then t1.col_4
            when t1.col_5 = '親会社の所有者に帰属する四半期利益' and t1.col_4 = '四半期利益' then t1.col_4    
            when t1.col_5 = '親会社の所有者に帰属する当期利益' and t1.col_4 = '当期利益' then t1.col_4   
            when t1.col_6 = '親会社の所有者に帰属する中間利益' and t1.col_5 = '中間利益' then t1.col_5
            when t1.col_6 = '親会社の所有者に帰属する四半期利益' and t1.col_5 = '四半期利益' then t1.col_5    
            when t1.col_6 = '親会社の所有者に帰属する当期利益' and t1.col_5 = '当期利益' then t1.col_5     
            when t1.col_5 = '当社株主に帰属する帰属する中間利益' and t1.col_4 = '中間利益' then t1.col_4
            when t1.col_5 = '当社株主に帰属する帰属する四半期利益' and t1.col_4 = '四半期利益' then t1.col_4    
            when t1.col_5 = '当社株主に帰属する帰属する当期利益' and t1.col_4 = '当期利益' then t1.col_4   
            when t1.col_6 = '当社株主に帰属する帰属する中間利益' and t1.col_5 = '中間利益' then t1.col_5
            when t1.col_6 = '当社株主に帰属する帰属する四半期利益' and t1.col_5 = '四半期利益' then t1.col_5    
            when t1.col_6 = '当社株主に帰属する帰属する当期利益' and t1.col_5 = '当期利益' then t1.col_5  
        end as ordinaly_profit_title,--経常利益
        case
            when t1.col_5 = '親会社の所有者に帰属する中間利益' and t1.col_6 = '親会社の所有者に帰属する中間利益' then t1.col_6 --ブリジストン(5108),継続事業のテーブルの下に非継続事業を含む、で同じ項目から始まる
            when t1.col_5 = '親会社の所有者に帰属する四半期利益' and t1.col_6 = '親会社の所有者に帰属する四半期利益' then t1.col_6
            when t1.col_5 = '親会社の所有者に帰属する当期利益' and t1.col_6 = '親会社の所有者に帰属する当期利益' then t1.col_6
            when regexp_contains(t1.col_5,r'親会社の所有者に帰属するコア') and regexp_contains(t1.col_12,r'親会社の所有者に帰属する') then t1.col_12
            when regexp_contains(t1.col_4,r'親会社の所有者に帰属する|親会社の所有に帰属する|当社株主に帰属する|当期株主に帰属する|親会社株主に帰属する|当社に帰属する') and not regexp_contains(t1.col_4,r'1株当たり|Non-GAAP|調整後|利益率') then t1.col_4
            when regexp_contains(t1.col_5,r'親会社の所有者に帰属する|親会社の所有に帰属する|当社株主に帰属する|当期株主に帰属する|親会社株主に帰属する|当社に帰属する') and not regexp_contains(t1.col_5,r'1株当たり|Non-GAAP|調整後|利益率') then t1.col_5
            when regexp_contains(t1.col_6,r'親会社の所有者に帰属する|親会社の所有に帰属する|当社株主に帰属する|当期株主に帰属する|親会社株主に帰属する|当社に帰属する') and not regexp_contains(t1.col_6,r'1株当たり|Non-GAAP|調整後|利益率') then t1.col_6
            when regexp_contains(t1.col_7,r'親会社の所有者に帰属する|親会社の所有に帰属する|当社株主に帰属する|当期株主に帰属する|親会社株主に帰属する|当社に帰属する') and not regexp_contains(t1.col_7,r'1株当たり|Non-GAAP|調整後|利益率') then t1.col_7
            when regexp_contains(t1.col_8,r'親会社の所有者に帰属する|親会社の所有に帰属する|当社株主に帰属する|当期株主に帰属する|親会社株主に帰属する|当社に帰属する') and not regexp_contains(t1.col_8,r'1株当たり|Non-GAAP|調整後|利益率') then t1.col_8
            when t1.col_4 = 'Non-GAAP経常利益' and t1.col_5 = 'Non-GAAP親会社株主に帰属する四半期純利益' then t1.col_5
            when t1.col_3 in('経常利益','経常損失','経常損益') and regexp_contains(t1.col_4,r'中間純利益|四半期純利益|当期純利益|中間純損失|四半期純損失|当期純損失|中間純損益|四半期純損益|当期純損益') then t1.col_4
            when t1.col_4 in('経常利益','経常損失','経常損益') and regexp_contains(t1.col_5,r'中間純利益|四半期純利益|当期純利益|中間純損失|四半期純損失|当期純損失|中間純損益|四半期純損益|当期純損益') then t1.col_5
            when t1.col_5 in('経常利益','経常損失','経常損益') and regexp_contains(t1.col_6,r'中間純利益|四半期純利益|当期純利益|中間純損失|四半期純損失|当期純損失|中間純損益|四半期純損益|当期純損益') then t1.col_6
            when t1.col_6 in('経常利益','経常損失','経常損益') and regexp_contains(t1.col_7,r'中間純利益|四半期純利益|当期純利益|中間純損失|四半期純損失|当期純損失|中間純損益|四半期純損益|当期純損益') then t1.col_7
        end as net_income_title,--純利益
        case
            when t1.col_2 in ('売上高','売上収益') and t1.col_3 = '営業収益' then t2.col_2 --2897
            when regexp_contains(t1.col_3,r'売上高|売上収益|保険収益|営業収益') then t2.col_3
            else t2.col_2 
        end as earnings,--売上高
        case 
            when t1.col_3 = 'コア営業利益' and t1.col_9 = '営業利益' then t2.col_9
            when t1.col_3 in ('コア営業利益','事業利益') and t1.col_4 like '税引前%' then t2.col_3
            when t1.col_4 like '税引前%' and t1.col_3 like '事業利益%' then t2.col_3
            when t1.col_5 like '税引前%' and t1.col_4 like '事業利益%' then t2.col_4
            when t1.col_5 in('営業利益','営業損失','営業活動に係る利益') then t2.col_5
            when t1.col_4 in('営業利益','営業損失','営業活動に係る利益') or t1.col_4 in('収益') then t2.col_4
            when t1.col_3 in('営業利益','営業損失','営業活動に係る利益') then t2.col_3
            when t1.col_2 = '保険収益' or t1.col_3 = '保険収益'  then null --7157,9630(保険業)
            when t1.col_3 = '収益合計(金融費用控除後)' then null --8604(野村)
            when t1.col_3 = 'EBITDA※1' and t1.col_4 = '事業利益※2' then t2.col_4 --3491　※2023年以前
            when t1.col_3 like '調整後営業利益%' and t1.col_4 != '営業利益' then t2.col_3 
            when t1.col_3 in ('経常利益','経常損益') or t1.col_3 like '税引前%' then null
            when regexp_contains(t1.col_3,r'営業利益|営業損失|営業収益') then t2.col_3--Non-GAAP営業利益とか営業損失(-)とか
        end as operating_income,--営業利益
        case    
            when t1.col_4 = 'コア四半期利益' and t1.col_10 = '税引前四半期利益' then t2.col_10
            when t1.col_4 = 'コア当期利益' and t1.col_10 = '税引前当期利益' then t2.col_10
            when regexp_contains(t1.col_3,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_3 not like '%利益率' then t2.col_3
            when regexp_contains(t1.col_4,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_4 not like '%利益率' then t2.col_4
            when regexp_contains(t1.col_5,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_5 not like '%利益率' then t2.col_5
            when regexp_contains(t1.col_6,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_6 not like '%利益率' then t2.col_6
            when regexp_contains(t1.col_7,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_7 not like '%利益率' then t2.col_7
            when regexp_contains(t1.col_8,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_8 not like '%利益率' then t2.col_8
            when t1.col_3 = '営業利益' and t1.col_4 in ('中間利益','四半期利益','親会社の所有者に帰属する中間利益','当期利益') then t2.col_4
            when t1.col_4 ='営業利益' and t1.col_5 in ('中間利益','四半期利益','親会社の所有者に帰属する中間利益','当期利益') then t2.col_5
            when t1.col_5 = '親会社の所有者に帰属する中間利益' and t1.col_4 = '中間利益' then t2.col_4
            when t1.col_5 = '親会社の所有者に帰属する四半期利益' and t1.col_4 = '四半期利益' then t2.col_4    
            when t1.col_5 = '親会社の所有者に帰属する当期利益' and t1.col_4 = '当期利益' then t2.col_4   
            when t1.col_6 = '親会社の所有者に帰属する中間利益' and t1.col_5 = '中間利益' then t2.col_5
            when t1.col_6 = '親会社の所有者に帰属する四半期利益' and t1.col_5 = '四半期利益' then t2.col_5    
            when t1.col_6 = '親会社の所有者に帰属する当期利益' and t1.col_5 = '当期利益' then t2.col_5     
            when t1.col_5 = '当社株主に帰属する帰属する中間利益' and t1.col_4 = '中間利益' then t2.col_4
            when t1.col_5 = '当社株主に帰属する帰属する四半期利益' and t1.col_4 = '四半期利益' then t2.col_4    
            when t1.col_5 = '当社株主に帰属する帰属する当期利益' and t1.col_4 = '当期利益' then t2.col_4   
            when t1.col_6 = '当社株主に帰属する帰属する中間利益' and t1.col_5 = '中間利益' then t2.col_5
            when t1.col_6 = '当社株主に帰属する帰属する四半期利益' and t1.col_5 = '四半期利益' then t2.col_5    
            when t1.col_6 = '当社株主に帰属する帰属する当期利益' and t1.col_5 = '当期利益' then t2.col_5  
        end as ordinaly_profit,--経常利益
        case
            when t1.col_5 = '親会社の所有者に帰属する中間利益' and t1.col_6 = '親会社の所有者に帰属する中間利益' then t2.col_6 --ブリジストン(5108),継続事業のテーブルの下に非継続事業を含む、で同じ項目から始まる
            when t1.col_5 = '親会社の所有者に帰属する四半期利益' and t1.col_6 = '親会社の所有者に帰属する四半期利益' then t2.col_6
            when t1.col_5 = '親会社の所有者に帰属する当期利益' and t1.col_6 = '親会社の所有者に帰属する当期利益' then t2.col_6
            when regexp_contains(t1.col_5,r'親会社の所有者に帰属するコア') and regexp_contains(t1.col_12,r'親会社の所有者に帰属する') then t2.col_12
            when regexp_contains(t1.col_4,r'親会社の所有者に帰属する|親会社の所有に帰属する|当社株主に帰属する|当期株主に帰属する|親会社株主に帰属する|当社に帰属する') and not regexp_contains(t1.col_4,r'1株当たり|Non-GAAP|調整後|利益率') then t2.col_4
            when regexp_contains(t1.col_5,r'親会社の所有者に帰属する|親会社の所有に帰属する|当社株主に帰属する|当期株主に帰属する|親会社株主に帰属する|当社に帰属する') and not regexp_contains(t1.col_5,r'1株当たり|Non-GAAP|調整後|利益率') then t2.col_5
            when regexp_contains(t1.col_6,r'親会社の所有者に帰属する|親会社の所有に帰属する|当社株主に帰属する|当期株主に帰属する|親会社株主に帰属する|当社に帰属する') and not regexp_contains(t1.col_6,r'1株当たり|Non-GAAP|調整後|利益率') then t2.col_6
            when regexp_contains(t1.col_7,r'親会社の所有者に帰属する|親会社の所有に帰属する|当社株主に帰属する|当期株主に帰属する|親会社株主に帰属する|当社に帰属する') and not regexp_contains(t1.col_7,r'1株当たり|Non-GAAP|調整後|利益率') then t2.col_7
            when regexp_contains(t1.col_8,r'親会社の所有者に帰属する|親会社の所有に帰属する|当社株主に帰属する|当期株主に帰属する|親会社株主に帰属する|当社に帰属する') and not regexp_contains(t1.col_8,r'1株当たり|Non-GAAP|調整後|利益率') then t2.col_8
            when t1.col_4 = 'Non-GAAP経常利益' and t1.col_5 = 'Non-GAAP親会社株主に帰属する四半期純利益' then t2.col_5
            when t1.col_3 in('経常利益','経常損失','経常損益') and regexp_contains(t1.col_4,r'中間純利益|四半期純利益|当期純利益|中間純損失|四半期純損失|当期純損失|中間純損益|四半期純損益|当期純損益') then t2.col_4
            when t1.col_4 in('経常利益','経常損失','経常損益') and regexp_contains(t1.col_5,r'中間純利益|四半期純利益|当期純利益|中間純損失|四半期純損失|当期純損失|中間純損益|四半期純損益|当期純損益') then t2.col_5
            when t1.col_5 in('経常利益','経常損失','経常損益') and regexp_contains(t1.col_6,r'中間純利益|四半期純利益|当期純利益|中間純損失|四半期純損失|当期純損失|中間純損益|四半期純損益|当期純損益') then t2.col_6
            when t1.col_6 in('経常利益','経常損失','経常損益') and regexp_contains(t1.col_7,r'中間純利益|四半期純利益|当期純利益|中間純損失|四半期純損失|当期純損失|中間純損益|四半期純損益|当期純損益') then t2.col_7
        end as net_income,--純利益
        case
            when t1.col_2 in ('売上高','売上収益') and t1.col_3 = '営業収益' then t3.col_2 --2897
            when regexp_contains(t1.col_3,r'売上高|売上収益|保険収益|営業収益') then t3.col_3
            else t3.col_2 
        end as before_earnings,--売上高
        case 
            when t1.col_3 = 'コア営業利益' and t1.col_9 = '営業利益' then t3.col_9
            when t1.col_3 in ('コア営業利益','事業利益') and t1.col_4 like '税引前%' then t3.col_3
            when t1.col_4 like '税引前%' and t1.col_3 like '事業利益%' then t3.col_3
            when t1.col_5 like '税引前%' and t1.col_4 like '事業利益%' then t3.col_4
            when t1.col_5 in('営業利益','営業損失','営業活動に係る利益') then t3.col_5
            when t1.col_4 in('営業利益','営業損失','営業活動に係る利益') or t1.col_4 in('収益') then t3.col_4
            when t1.col_3 in('営業利益','営業損失','営業活動に係る利益') then t3.col_3
            when t1.col_2 = '保険収益' or t1.col_3 = '保険収益'  then null --7157,9630(保険業)
            when t1.col_3 = '収益合計(金融費用控除後)' then null --8604(野村)
            when t1.col_3 = 'EBITDA※1' and t1.col_4 = '事業利益※2' then t3.col_4 --3491　※2023年以前
            when t1.col_3 like '調整後営業利益%' and t1.col_4 != '営業利益' then t3.col_3 
            when t1.col_3 in ('経常利益','経常損益') or t1.col_3 like '税引前%' then null
            when regexp_contains(t1.col_3,r'営業利益|営業損失|営業収益') then t3.col_3--Non-GAAP営業利益とか営業損失(-)とか
        end as before_operating_income,--営業利益
        case    
            when t1.col_4 = 'コア四半期利益' and t1.col_10 = '税引前四半期利益' then t3.col_10
            when t1.col_4 = 'コア当期利益' and t1.col_10 = '税引前当期利益' then t3.col_10
            when regexp_contains(t1.col_3,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_3 not like '%利益率' then t3.col_3
            when regexp_contains(t1.col_4,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_4 not like '%利益率' then t3.col_4
            when regexp_contains(t1.col_5,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_5 not like '%利益率' then t3.col_5
            when regexp_contains(t1.col_6,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_6 not like '%利益率' then t3.col_6
            when regexp_contains(t1.col_7,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_7 not like '%利益率' then t3.col_7
            when regexp_contains(t1.col_8,r'税引前|税金等調整前|経常利益|経常損失|経常損益|法人税等') and t1.col_8 not like '%利益率' then t3.col_8
            when t1.col_3 = '営業利益' and t1.col_4 in ('中間利益','四半期利益','親会社の所有者に帰属する中間利益','当期利益') then t3.col_4
            when t1.col_4 ='営業利益' and t1.col_5 in ('中間利益','四半期利益','親会社の所有者に帰属する中間利益','当期利益') then t3.col_5
            when t1.col_5 = '親会社の所有者に帰属する中間利益' and t1.col_4 = '中間利益' then t3.col_4
            when t1.col_5 = '親会社の所有者に帰属する四半期利益' and t1.col_4 = '四半期利益' then t3.col_4    
            when t1.col_5 = '親会社の所有者に帰属する当期利益' and t1.col_4 = '当期利益' then t3.col_4   
            when t1.col_6 = '親会社の所有者に帰属する中間利益' and t1.col_5 = '中間利益' then t3.col_5
            when t1.col_6 = '親会社の所有者に帰属する四半期利益' and t1.col_5 = '四半期利益' then t3.col_5    
            when t1.col_6 = '親会社の所有者に帰属する当期利益' and t1.col_5 = '当期利益' then t3.col_5     
            when t1.col_5 = '当社株主に帰属する帰属する中間利益' and t1.col_4 = '中間利益' then t3.col_4
            when t1.col_5 = '当社株主に帰属する帰属する四半期利益' and t1.col_4 = '四半期利益' then t3.col_4    
            when t1.col_5 = '当社株主に帰属する帰属する当期利益' and t1.col_4 = '当期利益' then t3.col_4   
            when t1.col_6 = '当社株主に帰属する帰属する中間利益' and t1.col_5 = '中間利益' then t3.col_5
            when t1.col_6 = '当社株主に帰属する帰属する四半期利益' and t1.col_5 = '四半期利益' then t3.col_5    
            when t1.col_6 = '当社株主に帰属する帰属する当期利益' and t1.col_5 = '当期利益' then t3.col_5  
        end as before_ordinaly_profit,--経常利益
        case
            when t1.col_5 = '親会社の所有者に帰属する中間利益' and t1.col_6 = '親会社の所有者に帰属する中間利益' then t3.col_6 --ブリジストン(5108),継続事業のテーブルの下に非継続事業を含む、で同じ項目から始まる
            when t1.col_5 = '親会社の所有者に帰属する四半期利益' and t1.col_6 = '親会社の所有者に帰属する四半期利益' then t3.col_6
            when t1.col_5 = '親会社の所有者に帰属する当期利益' and t1.col_6 = '親会社の所有者に帰属する当期利益' then t3.col_6
            when regexp_contains(t1.col_5,r'親会社の所有者に帰属するコア') and regexp_contains(t1.col_12,r'親会社の所有者に帰属する') then t3.col_12
            when regexp_contains(t1.col_4,r'親会社の所有者に帰属する|親会社の所有に帰属する|当社株主に帰属する|当期株主に帰属する|親会社株主に帰属する|当社に帰属する') and not regexp_contains(t1.col_4,r'1株当たり|Non-GAAP|調整後|利益率') then t3.col_4
            when regexp_contains(t1.col_5,r'親会社の所有者に帰属する|親会社の所有に帰属する|当社株主に帰属する|当期株主に帰属する|親会社株主に帰属する|当社に帰属する') and not regexp_contains(t1.col_5,r'1株当たり|Non-GAAP|調整後|利益率') then t3.col_5
            when regexp_contains(t1.col_6,r'親会社の所有者に帰属する|親会社の所有に帰属する|当社株主に帰属する|当期株主に帰属する|親会社株主に帰属する|当社に帰属する') and not regexp_contains(t1.col_6,r'1株当たり|Non-GAAP|調整後|利益率') then t3.col_6
            when regexp_contains(t1.col_7,r'親会社の所有者に帰属する|親会社の所有に帰属する|当社株主に帰属する|当期株主に帰属する|親会社株主に帰属する|当社に帰属する') and not regexp_contains(t1.col_7,r'1株当たり|Non-GAAP|調整後|利益率') then t3.col_7
            when regexp_contains(t1.col_8,r'親会社の所有者に帰属する|親会社の所有に帰属する|当社株主に帰属する|当期株主に帰属する|親会社株主に帰属する|当社に帰属する') and not regexp_contains(t1.col_8,r'1株当たり|Non-GAAP|調整後|利益率') then t3.col_8
            when t1.col_4 = 'Non-GAAP経常利益' and t1.col_5 = 'Non-GAAP親会社株主に帰属する四半期純利益' then t3.col_5
            when t1.col_3 in('経常利益','経常損失','経常損益') and regexp_contains(t1.col_4,r'中間純利益|四半期純利益|当期純利益|中間純損失|四半期純損失|当期純損失|中間純損益|四半期純損益|当期純損益') then t3.col_4
            when t1.col_4 in('経常利益','経常損失','経常損益') and regexp_contains(t1.col_5,r'中間純利益|四半期純利益|当期純利益|中間純損失|四半期純損失|当期純損失|中間純損益|四半期純損益|当期純損益') then t3.col_5
            when t1.col_5 in('経常利益','経常損失','経常損益') and regexp_contains(t1.col_6,r'中間純利益|四半期純利益|当期純利益|中間純損失|四半期純損失|当期純損失|中間純損益|四半期純損益|当期純損益') then t3.col_6
            when t1.col_6 in('経常利益','経常損失','経常損益') and regexp_contains(t1.col_7,r'中間純利益|四半期純利益|当期純利益|中間純損失|四半期純損失|当期純損失|中間純損益|四半期純損益|当期純損益') then t3.col_7
        end as before_net_income--純利益
    from
        header_tb as t1
    left join
        after_tb as t2
        on t1.xbrl = t2.xbrl
    left join
        before_tb as t3
        on t1.xbrl = t3.xbrl
),
base as(--beforeを掲載していない企業がある
    select
        t1.*,
        t2.* except(xbrl,earnings,operating_income,ordinaly_profit,net_income,before_earnings,before_operating_income,before_ordinaly_profit,before_net_income),
        cast(replace(replace(earnings,',',''),'(※)','') as float64) as earnings,
        cast(replace(replace(operating_income,',',''),'(※)','') as float64) as operating_income,
        cast(replace(replace(ordinaly_profit,',',''),'(※)','') as float64) as ordinaly_profit,
        cast(replace(replace(net_income,',',''),'(※)','') as float64) as net_income,
        cast(replace(replace(before_earnings,',',''),'(※)','') as float64) as before_earnings,
        cast(replace(replace(before_operating_income,',',''),'(※)','') as float64) as before_operating_income,
        cast(replace(replace(before_ordinaly_profit,',',''),'(※)','') as float64) as before_ordinaly_profit,
        cast(replace(replace(before_net_income,',',''),'(※)','') as float64) as before_net_income,
        total_assets,
        net_assets,
        stock_amount,
        stock_reward,
    from
        jpx.refine_securities_report_master as t1
    left join
        summary_tb as t2
        on t1.xbrl = t2.xbrl
    left join
        `jpx.others_*` as t3
        on t1.xbrl = t3.xbrl
),
num_add as(--修正を含め四半期ごとの最終行を取得
    select
        *,
        row_number() over(partition by stock_code,period,quarter order by release_date desc,refine_flg desc) as row_number
    from
        base
),
final_data as(--最終的なperiod,quarterの値のみにする
    select
        * 
    from
        num_add
    where
        row_number = 1
)
--beforeがない場合前期の値をbeforeとして取得
select 
    t1.* except(xbrl,title,inpage_title,before_earnings,before_operating_income,before_ordinaly_profit,before_net_income),
    coalesce(t1.before_earnings,t2.earnings) as before_earnings,
    coalesce(t1.before_operating_income,t2.operating_income) as before_operating_income,
    coalesce(t1.before_ordinaly_profit,t2.ordinaly_profit) as before_ordinaly_profit,
    coalesce(t1.before_net_income,t2.net_income) as before_net_income
from 
    base as t1
left join
    final_data as t2
    on t1.stock_code = t2.stock_code and t1.period -1 = t2.period and t1.quarter = t2.quarter
)
