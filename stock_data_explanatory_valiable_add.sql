drop table looker_datamart.stock_data_explanatory_valiable_add ;
create table looker_datamart.stock_data_explanatory_valiable_add 
partition by created_at as(
with
base as(
    select
        cast(date as date) as created_at,
        *,
        lead(open,1) over(partition by stock_code order by cast(date as date)) as contract_price,--翌日の始値が約定価格
        row_number() over(partition by stock_code order by cast(date as date)) as row_number
    from
        stock_data.tokyo_01
),
max_add as(
    select
        *,
        max(high) over(partition by stock_code order by row_number rows between current row and +90 following) as max_high,
        min(low) over(partition by stock_code order by row_number rows between current row and +90 following) as min_low,
    from
        base 
),
flg_add as(
    select
        *,
        case when max_high / contract_price >= 1.1 then 1 end as win_flg,
        case when min_low / contract_price <= 0.94 then 1 end as lose_flg,
    from
        max_add
),
quartely_report_add as(
    select
        t1.*,
        t2.* except(stock_code,date,refine_flg,period,quarter,earnings,operating_income,ordinaly_profit,net_income,release_date,next_release_date),
        case
            when date_diff(t1.created_at,release_date,day) <= 120 then row_number() over(partition by t1.stock_code,release_date order by t1.created_at) 
        end as past_day
    from
        flg_add as t1
    left join
        securities_report.quartely_report_for_learning as t2
        on t1.stock_code = t2.stock_code and t1.created_at between date_add(t2.release_date,interval +1 day) and t2.next_release_date 
        and date_diff(t1.created_at,release_date,day) <= 120 --決算短信の掲載漏れがある・・・！？　四半期ごとに出ているはずなので漏れがあったら結合しない
),
data_tb as(
    select
        *,
        close / nullif(((quarter_net_income*1000000 * 4)/nullif(stock_amount,0)),0) as per,--株価収益率
        close / nullif(((net_assets*1000000)/nullif(stock_amount,0)),0) as pbr,--株価純資産倍率　※2024年版ではnet_assets*1000000*4になっていた,net_assetsは純資産だから四半期ごとの値じゃないので4倍してはいけない
        (quarter_net_income*1000000 * 4) / ((total_assets*1000000) * nullif((equity_ratio/100),0))  as roe,--自己資本利益率
        (quarter_net_income*1000000 * 4) / nullif((total_assets*1000000),0) as roa,--総資産利益率    
        close * stock_amount as market_cap--時価総額    
    from
        quartely_report_add
),
base_aggre as(--各テクニカル指標の元となる値を集計
    select *,
        avg(close) over(partition by stock_code order by created_at rows between 6 preceding and current row) as short_avg, --7日間平均 
        avg(close) over(partition by stock_code order by created_at rows between 27 preceding and current row) as long_avg, --28日間平均
        lag(close,1) over(partition by stock_code order by created_at) as before_close, --前日の値
        lag(close,2) over(partition by stock_code order by created_at) as before_day3_close, 
        lag(close,3) over(partition by stock_code order by created_at) as before_day4_close, 
        lag(close,4) over(partition by stock_code order by created_at) as before_day5_close, 
        lag(close,5) over(partition by stock_code order by created_at) as before_day6_close, 
        lag(close,6) over(partition by stock_code order by created_at) as before_day7_close, --7日前の値
        min(close) over(partition by stock_code order by created_at rows between 6 preceding and current row) as range_min,--直近7日間の最安値,
        max(close) over(partition by stock_code order by created_at rows between 6 preceding and current row) as range_max,--直近7日間の最高値,
    from 
        data_tb    
),
trend_add as(--移動平均のクロスを判別するフラグや各指標の元となる値を引き続き集計
    select *,
        case when short_avg >= long_avg then 'upper' else 'lower' end as trend,--7日間平均が28日平均を上回っていればupper
        case when close > before_close then close - before_close else 0 end as gain,
        case when close < before_close then before_close - close else 0 end as loss,
        min(close) over(partition by stock_code order by created_at rows between 750 preceding and current row) as min_3year_close,--直近3年間の最安値
        max(close) over(partition by stock_code order by created_at rows between 750 preceding and current row) as max_3year_close,--直近3年間の最安値
        min(created_at) over(partition by stock_code order by created_at rows between 750 preceding and current row) min_3year_day,--直近3年間の最初の日
        (close - range_min) / nullif((range_max - range_min),0) as k_value,
        ((close - ifnull(before_day7_close,0)) / nullif(before_day7_close,0)) * 100 as roc,
        case when close > before_close then 1 else 0 end as day2_cnt,
        case when close > before_day3_close then 1 else 0 end as day3_cnt,
        case when close > before_day4_close then 1 else 0 end as day4_cnt,
        case when close > before_day5_close then 1 else 0 end as day5_cnt,
        case when close > before_day6_close then 1 else 0 end as day6_cnt,
        case when close > before_day7_close then 1 else 0 end as day7_cnt,
        case
            when close > before_close then 'up'
            when close < before_close then 'down'
            else 'stay'
        end as price_movement,
    from 
        base_aggre
),
trend_lag_add as(--クロス発生か否かを判別するため、前日のフラグ付与,rsiはここで完成
    select 
        *,
        lag(trend,1) over(partition by stock_code order by created_at) as before_trend,
        avg(gain) over (partition by stock_code order by created_at rows between 13 preceding and current row) as avg_gain,
        avg(loss) over (partition by stock_code order by created_at rows between 13 preceding and current row) as avg_loss,
        sum(k_value) over(partition by stock_code order by created_at rows between 3 preceding and current row) / 4 as d_value,
        count(case when close - before_close > 0 then close end) over(partition by stock_code order by created_at rows between 11 preceding and current row) as upper_days,
        day2_cnt + day3_cnt + day4_cnt + day5_cnt + day6_cnt + day7_cnt +1 as close_rank,
        ifnull(sum(case when price_movement = 'up' then volume else 0 end) over(partition by stock_code order by created_at rows between 19 preceding and current row),0) as up_volume,
        ifnull(sum(case when price_movement = 'down' then volume else 0 end) over(partition by stock_code order by created_at rows between 19 preceding and current row),0) as down_volume,
        ifnull(sum(case when price_movement = 'stay' then volume else 0 end) over(partition by stock_code order by created_at rows between 19 preceding and current row),0) as stay_volume

    from 
        trend_add
),
sign_add as(--前日のフラグと異なるなら売買サイン,stcasticksも移動平均と同様クロス判別が必要なのでここでフラグ建て
    select *,
    case when trend = 'upper' and before_trend = 'lower' then 1
         when trend = 'lower' and before_trend = 'upper' then 4
         when trend = 'upper' then 2
         when trend = 'lower' then 3
    end as moving_avg,
    case when k_value >= d_value then 'upper' else 'lower' end as stocas_trend,
    (upper_days / 12) * 100 as psychological,
    case
        when avg_loss = 0 then 100
        else 100 - (100 / (1 + (avg_gain / avg_loss)))
    end as rsi,
    lag(close_rank,1) over(partition by stock_code order by created_at) as day1_close_rank,
    lag(close_rank,2) over(partition by stock_code order by created_at) as day2_close_rank,
    lag(close_rank,3) over(partition by stock_code order by created_at) as day3_close_rank,
    lag(close_rank,4) over(partition by stock_code order by created_at) as day4_close_rank,
    lag(close_rank,5) over(partition by stock_code order by created_at) as day5_close_rank,
    lag(close_rank,6) over(partition by stock_code order by created_at) as day6_close_rank,
    ((up_volume + (stay_volume/2)) / nullif((down_volume + (stay_volume/2)),0)) *100 as volume_ratio
    from  trend_lag_add
),
sign_add2 as(
    select *,
    lag(stocas_trend,1) over(partition by stock_code order by created_at) as before_stocas ,
    ifnull(pow((7- close_rank),2),0) + ifnull(pow((6- day1_close_rank),2),0) + ifnull(pow((5- day2_close_rank),2),0) + ifnull(pow((4- day3_close_rank),2),0) + ifnull(pow((3- day4_close_rank),2),0)
    + ifnull(pow((2- day5_close_rank),2),0) + ifnull(pow((1- day6_close_rank),2),0) as rci_d_value
    from sign_add
),
sign_add3 as(
    select t1.*,
    case when stocas_trend = 'upper' and before_stocas = 'lower' then 1
         when stocas_trend = 'lower' and before_stocas = 'upper' then 4
         when stocas_trend = 'upper' then 2
         when stocas_trend = 'lower' then 3
    end as stocasticks,
    cast((1 - ((rci_d_value * 6) / (7*48))) * 100 as int64) as rci,  --分母はn(nの2乗-1),7日なので7*48
    close / nullif(long_avg,0) as envelope,
    case
        when date_add(created_at,interval - 100 day) <= min_3year_day then null   --当日から100日引いた日付　より　直近3年間の営業日の最小日が大きいならnull
        else close / min_3year_close 
    end as bottom_relative_rate,
    case
        when date_add(created_at,interval - 100 day) <= min_3year_day then null   --当日から100日引いた日付　より　直近3年間の営業日の最小日が大きいならnull
        else close / max_3year_close 
    end as top_relative_rate,
    t2.type1
    from 
        sign_add2 as t1
    left join
        stock_data_mst.stock_data_mst_tokyo_01 as t2
        on t1.stock_code = t2.code        
),
avg_aggre as(
    select
        type1,
        avg(per) as avg_per,
        stddev(per) as std_per
    from
        sign_add3
    group by 1
),
minkabu_tb as(
    select 
        *,
        _table_suffix as suffix,
        max(_table_suffix) over() as max_suffix 
    from 
        `finance_datamart.minkabu_light_*`
    where parse_date('%Y%m%d',_table_suffix) between date_add(current_date('Asia/Tokyo'),interval -7 day) and current_date('Asia/Tokyo')
),
minkabu as (
    select
        *
    from
        minkabu_tb
    where
        suffix = max_suffix
)
select
    t1.created_at,
    t1.stock_code,
    t1.close,
    t1.volume,
    t1.past_day,
    contract_price,--約定値段
    win_flg,
    lose_flg,
    extract(month from created_at) as month,
    stock_reward / nullif(close,0) as reward_rate,
    stock_reward,--配当
    quarter_earnings_rate,--売上高(前年同期比)
    quarter_operating_income_rate,--営業利益(前年同期比)
    quarter_operating_income_gain_rate,--営業利益率(前年同期比)
    quarter_ordinaly_profit_rate,--経常利益(前年同期比)
    quarter_net_income_rate,--純利益(前年同期比)
    50 + 10 * (t1.per - t2.avg_per) / std_per as deviation_per, --業種別PER偏差
    pbr,--株価純資産倍率
    roe,--自己資本利益率
    roa,--総資産利益率
    moving_avg,--移動平均
    rsi,--14日間のRSI
    stocasticks,
    volume_ratio,--nullの場合フラグを立てることにした
    psychological,
    roc,
    rci,
    envelope,
    bottom_relative_rate,--直近3年の最安値に対する元終値の割合
    top_relative_rate,--同上の最高値
    row_number() over(partition by t1.stock_code order by created_at) as day_number,
    case when date_diff(t1.created_at,t3.ipo_date,day) <= 365 then 1 else 0 end as ipo_flg, --上場日から直近1年間にフラグ
    case when volume_ratio is null then 1 else 0 end as vr_null_flg,
    case 
        when market_cap >= 500000000000 then 'large'
        when market_cap >= 200000000000 then 'mid'
        else 'small'
    end as market_cap_section
from 
    sign_add3 as t1
left join
    avg_aggre as t2
    on t1.type1 = t2.type1
left join
    minkabu as t3
    on t1.stock_code = t3.stock_code
);


