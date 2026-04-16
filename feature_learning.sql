create or replace table looker_datamart.feature_learning
partition by created_at
cluster by stock_code as(
with
stock_data as(
    select 
        created_at,
        stock_code,
        close,
        volume,
        quarter_net_income_rate,
        roa,
        roe,
        pbr,
        per,
        reward_rate,
        win_flg,
        lose_flg,
        weather,
        supervision_reason,
        ipo_flg,
        irregular_flg,
        stock_split,
        buyback_flg
    from 
        looker_datamart.stock_data_explanatory_valiable_add
    where 
        created_at >= '2016-06-01'
),
joint_tb as(
    select
        t1.*,
        t2.forcast_up_rate,
        t3.forcast_down_rate,
        row_number() over(partition by t1.stock_code order by t1.created_at) as row_number
    from
        stock_data  as t1
    left join
        temp_folder.feature_learning_up as t2
        on t1.stock_code = t2.stock_code and t1.created_at = t2.created_at
    left join
        temp_folder.feature_learning_down as t3
        on t1.stock_code = t3.stock_code and t1.created_at = t3.created_at
),
suggest_add as(
    select
        *,
        case
            when pbr >= 0 and forcast_up_rate >= 0.9 and forcast_down_rate < 0.1 and supervision_reason is null and irregular_flg is null then
            case
                when stock_split in(2,3) then '株式分割'
                when buyback_flg = 1         then '自社株買'
                when reward_rate >= 0.04     then '高配当'                
                when weather in(4,5)         then '優良企業'  
                else '確率のみ'              
            end            
        end as suggest_type,
        case
            when pbr >= 0 and forcast_up_rate >= 0.9 and forcast_down_rate < 0.1 and supervision_reason is null and irregular_flg is null then
            case
                when stock_split in(2,3) then 1
                when buyback_flg = 1         then 2
                when reward_rate >= 0.04     then 3                
                when weather in(4,5)         then 4  
                else 5              
            end
        end as suggest_sort
    from
        joint_tb
),
lag_add as(
    select
        *,
        lag(row_number,1) over(partition by stock_code,suggest_type order by created_at) as last_row_number
    from
        suggest_add
)
select
    *,
    case when (last_row_number is null or row_number - last_row_number >= 3) and suggest_type is not null then 1 end as dual_num
from
    lag_add
)






