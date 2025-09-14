create or replace table looker_datamart.stock_data_explanatory_valiable_add
partition by created_at as(
with
delisting_tb as(
    select
        *
    from
        `stock_data_delisting.*`
),
delisting_stock_code as(
    select
        distinct
        stock_code
    from
        delisting_tb
),
delisting_mst as(
    select
        stock_code
    from
        `stock_data_mst.delisting_20*`
    where
        end_date <= current_date('Asia/Tokyo')
),
tokyo_01 as(
    select
        t1.*
    from
        stock_data.tokyo_01 as t1
    left join
        delisting_mst as t2
        on t1.stock_code = t2.stock_code
    left join
        delisting_stock_code as t3
        on t1.stock_code = t3.stock_code
    where
        t2.stock_code is null and t3.stock_code is null
),
all_stock_data as(
    select * from tokyo_01 union all
    select * from delisting_tb
),
base as(
    select
        cast(date as date) as created_at,
        *,
        lead(open,1) over(partition by stock_code order by cast(date as date)) as contract_price,--翌日の始値が約定価格
        row_number() over(partition by stock_code order by cast(date as date)) as row_number
    from
        all_stock_data 
),
max_add as(
    select
        *,
        max(high) over(partition by stock_code order by row_number rows between +1 following and +90 following) as max_high,
        min(low) over(partition by stock_code order by row_number rows between +1 following and +90 following) as min_low,
    from
        base 
),
flg_add as(
    select
        *,
        case when max_high / contract_price >= 1.1 then 1 end as win_flg,
        case when min_low / contract_price <= 0.94 then 1 end as pre_lose_flg,
    from
        max_add
),
pre_supervision as(--管理銘柄のテーブル
    select
        * except(url,excluded,name),
        ifnull(date_add(lead(release_date,1) over(partition by stock_code order by release_date),interval -1 day),current_date('Asia/Tokyo')) as end_date
    from    
        `stock_data.supervision_*`
),
supervision as(
    select
        * ,
        case
            when status = '指定' then reason
        end as supervision_reason
    from
        pre_supervision
),
buyback_join as(--期間がかぶることがある
    select
        t1.*,        
        case 
            when date_diff(t1.created_at,t2.start_date,day) between -20 and 0 then 1
        end as buyback_flg
    from
        flg_add as t1
    left join
        `stock_data.buyback_*` as t2
        on t1.stock_code = t2.stock_code and t1.created_at between t2.release_date and t2.end_date
),
buyback_unique as(
    select
        created_at,
        stock_code,
        case when sum(buyback_flg) > 0 then 1 end as buyback_flg
    from
        buyback_join
    group by 1,2
),
quartely_report as(
    select
        *,
        (net_income - lag(net_income,1) over(partition by stock_code,quarter order by period) )/ nullif(abs(lag(net_income,1) over(partition by stock_code,quarter order by period)),0) as net_income_gain_rate, --前年同期比の純利益増減率
    from
        securities_report.quartely_report_for_learning
),
quarter4 as(--4期のみにする
    select
        *
    from
        quartely_report
    where
        quarter = 4
),
decrease_add as(--減益となった場合フラグを立てる
    select
        *,
        case
            when net_income - lag(net_income,1) over(partition by stock_code order by date) < 0 then 1
        end as decrease_flg,
        case
            when net_income - lag(net_income,1) over(partition by stock_code order by date) > 0 then 1
        end as increase_flg,        
     from
        quarter4
),
decrease_running as(--減益となった累計回数を付与
    select
        *,
        sum(increase_flg) over(partition by stock_code order by date) as running_increase,
        sum(decrease_flg) over(partition by stock_code order by date) as running_decrease
    from
        decrease_add
),
increase_num_add as(--累計減益回数毎の累計行数=連続増益回数を集計
    select
        *,
        sum(increase_flg) over(partition by stock_code,running_decrease order by date) as increase_num,--partition by が逆にしてある点に注意
        sum(decrease_flg) over(partition by stock_code,running_increase order by date) * -1 as decrease_num,--逆にすることで累計が成立する
        max(period) over(partition by stock_code) as max_period
    from
        decrease_running
),
increase_num_fin as(
    select
        * except(increase_num,decrease_num),
        coalesce(increase_num,decrease_num) as increase_num
    from
        increase_num_add
),
max_period_only as(--最新の4期のみにする
    select
        *
    from
        increase_num_fin
    where
        period = max_period
),
quartely_report_with_increase_num as(--最新の期が4期でない場合nullとなってしまうのでその場合最新の4期の連続増益回数を付与
    select
        t1.*,
        coalesce(t2.increase_num,t3.increase_num) as increase_num,
    from
        quartely_report as t1
    left join
        increase_num_add as t2
        on t1.stock_code = t2.stock_code and t1.period = t2.period
    left join
        max_period_only as t3
        on t1.stock_code = t3.stock_code
),
quartely_report_add as(
    select
        t1.*,
        date_diff(t1.created_at,t2.release_date,day) as report_release_past_day,
        case when t1.win_flg is null and t1.pre_lose_flg = 1 then 1 end as lose_flg,
        t2.* except(stock_code,date,refine_flg,period,quarter,earnings,operating_income,ordinaly_profit,net_income,release_date,next_release_date),
        (t3.split_stock_amount + t3.exist_stock_amount) / t3.exist_stock_amount as split_rate,
        t3.release_date as split_release_date,
        t4.buyback_flg,
        t5.supervision_reason,
        t5.release_date as supervision_release_date
    from
        flg_add as t1
    left join
        quartely_report_with_increase_num as t2
        on t1.stock_code = t2.stock_code and t1.created_at between t2.release_date and date_add(t2.next_release_date,interval -1 day) --1日遅延させていたのを解消
        and date_diff(t1.created_at,t2.release_date,day) <= 120 --決算短信の掲載漏れがある・・・！？　四半期ごとに出ているはずなので漏れがあったら結合しない
    left join
        `stock_data.cleanging_stock_split` as t3
        on t1.stock_code = t3.stock_code and t1.created_at = t3.release_date
    left join
        buyback_unique as t4
        on t1.stock_code = t4.stock_code and t1.created_at = t4.created_at
    left join supervision as t5
        on t1.stock_code = t5.stock_code and t1.created_at between t5.release_date and t5.end_date
),
data_tb as(
    select
        *,
        close / nullif(((quarter_net_income*1000000 * 4)/nullif(stock_amount,0)),0) as per,--株価収益率
        close / nullif(((net_assets*1000000)/nullif(stock_amount,0)),0) as pbr,--株価純資産倍率　※2024年版ではnet_assets*1000000*4になっていた,net_assetsは純資産だから四半期ごとの値じゃないので4倍してはいけない
        (quarter_net_income*1000000 * 4) / ((total_assets*1000000) * nullif((equity_ratio/100),0))  as roe,--自己資本利益率
        (quarter_net_income*1000000 * 4) / nullif((total_assets*1000000),0) as roa,--総資産利益率    
        close * stock_amount as market_cap,--時価総額 
        max(split_release_date) over(partition by stock_code order by created_at) as running_release_date,   
        date_diff(created_at,supervision_release_date,day) as supervision_past_day,
        EXP(SUM(LOG(IFNULL(split_rate,1))) OVER (
            PARTITION BY stock_code
            ORDER BY created_at desc
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS cum_split_rate --利回りを調整後利回りにするための累積値
    from
        quartely_report_add
),
base_aggre as(--各テクニカル指標の元となる値を集計
    select t1.* except(split_rate,stock_reward,cum_split_rate),
        avg(t1.close) over(partition by t1.stock_code order by t1.created_at rows between 6 preceding and current row) as short_avg, --7日間平均 
        avg(t1.close) over(partition by t1.stock_code order by t1.created_at rows between 27 preceding and current row) as long_avg, --28日間平均
        lag(t1.close,1) over(partition by t1.stock_code order by t1.created_at) as before_close, --前日の値
        lag(t1.close,2) over(partition by t1.stock_code order by t1.created_at) as before_day3_close, 
        lag(t1.close,3) over(partition by t1.stock_code order by t1.created_at) as before_day4_close, 
        lag(t1.close,4) over(partition by t1.stock_code order by t1.created_at) as before_day5_close, 
        lag(t1.close,5) over(partition by t1.stock_code order by t1.created_at) as before_day6_close, 
        lag(t1.close,6) over(partition by t1.stock_code order by t1.created_at) as before_day7_close, --7日前の値
        min(t1.close) over(partition by t1.stock_code order by t1.created_at rows between 6 preceding and current row) as range_min,--直近7日間の最安値,
        max(t1.close) over(partition by t1.stock_code order by t1.created_at rows between 6 preceding and current row) as range_max,--直近7日間の最高値,
        t2.split_rate,
        t1.stock_reward / t1.cum_split_rate as stock_reward, --調整後配当
    from 
        data_tb as t1
    left join
        data_tb as t2
        on t1.stock_code = t2.stock_code and t1.running_release_date = t2.split_release_date
),
trend_add as(--移動平均のクロスを判別するフラグや各指標の元となる値を引き続き集計
    select * except(split_release_date,running_release_date),
        case when short_avg >= long_avg then 'upper' else 'lower' end as trend,--7日間平均が28日平均を上回っていればupper
        case when close > before_close then close - before_close else 0 end as gain,
        case when close < before_close then before_close - close else 0 end as loss,
        min(close) over(partition by stock_code order by created_at rows between 60 preceding and current row) as min_60day_close,--直近N日の最安値
        max(close) over(partition by stock_code order by created_at rows between 60 preceding and current row) as max_60day_close,--直近N日の最安値
        min(close) over(partition by stock_code order by created_at rows between 750 preceding and current row) as min_3year_close,--直近N日の最安値
        max(close) over(partition by stock_code order by created_at rows between 750 preceding and current row) as max_3year_close,--直近N日の最安値
        min(created_at) over(partition by stock_code) min_dt,
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
        date_diff(created_at,running_release_date,day) as release_past_day,
        (close - before_close) / close as daily_volatility,
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
        ifnull(sum(case when price_movement = 'stay' then volume else 0 end) over(partition by stock_code order by created_at rows between 19 preceding and current row),0) as stay_volume,
        case 
            when release_past_day <= 30 then
                case
                    when split_rate < 2 then 1
                    when split_rate < 5 then 2
                    when split_rate >= 5 then 3
            end 
        end as stock_split,
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
        when date_diff(created_at,min_dt,day) < 60 then null   --N日経過していないならnull
        else close / min_60day_close 
    end as day60_bottom_relative_rate,
    case
        when date_diff(created_at,min_dt,day) < 60 then null   --N日経過していないならnull
        else close / max_60day_close 
    end as day60_top_relative_rate,
    case
        when date_diff(created_at,min_dt,day) < 100 then null   --N日経過していないならnull
        else close / min_3year_close 
    end as bottom_relative_rate,
    case
        when date_diff(created_at,min_dt,day) < 100 then null   --N日経過していないならnull
        else close / max_3year_close 
    end as top_relative_rate,
    t2.type1
    from 
        sign_add2 as t1
    left join
        stock_data_mst.stock_data_mst_tokyo_01 as t2
        on t1.stock_code = t2.code        
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
    contract_price,--約定値段
    win_flg,
    lose_flg,
    (max_high - close) / close as max_high_rate,
    (min_low - close) / close as min_low_rate,
    extract(month from created_at) as month,
    stock_reward / nullif(close,0) as reward_rate, --調整後利回り
    stock_reward,--調整後配当
    quarter_earnings_rate,--売上高(前年同期比)
    quarter_operating_income_rate,--営業利益(前年同期比)
    quarter_operating_income_gain_rate,--営業利益率(前年同期比)
    quarter_ordinaly_profit_rate,--経常利益(前年同期比)
    quarter_net_income_rate,--純利益(前年同期比)
    per, --per
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
    day60_bottom_relative_rate,
    day60_top_relative_rate,
    row_number() over(partition by t1.stock_code order by created_at) as day_number,
    case when date_diff(t1.created_at,t2.ipo_date,day) <= 365 then 1 else 0 end as ipo_flg, --上場日から直近1年間にフラグ
    case 
        when market_cap >= 500000000000 then 'large'
        when market_cap >= 200000000000 then 'mid'
        else 'small'
    end as market_cap_section,--時価総額
    stock_split,--株式分割(カテゴリ),1:1-1未満,2:1-5未満,3:1-5以上
    buyback_flg,--自社株買い(カテゴリ),買いが開始される20日前～当日にフラグが立つ ※多くの場合は前営業日に発表ではある
    supervision_reason,--管理銘柄になっている理由
    supervision_past_day,
    case
        when long_avg < 50 then 1
        when long_avg < 300 then 2
        when long_avg < 1000 then 3
        when long_avg < 5000 then 4
        else 5
    end as price_range,--価格帯
    case 
        when daily_volatility < -0.15 then 1
        when daily_volatility < -0.1 then 2
        when daily_volatility < -0.02 then 3
        when daily_volatility < 0.02 then 4
        when daily_volatility < 0.1 then 5
        when daily_volatility < 0.15 then 6
        when daily_volatility >= 0.15 then 7
    end as volatility,
    case
        when report_release_past_day <= 90 then --4半期に一度なので90日以内で限定
            case
                when net_income_gain_rate is null then 2
                when net_income_gain_rate < -5 then 3 -- -500%未満
                when net_income_gain_rate < -1 then 4 -- -100%未満
                when net_income_gain_rate < -0.2 then 5 -- -20%未満
                when net_income_gain_rate < 2 then 6 -- 200%未満
                when net_income_gain_rate >= 2 then 7 --200%未満
            end
        else 1
    end as net_income_gain_flg,
    case 
        when increase_num <= -3 then -3
        when increase_num >= 3 then 3
        else increase_num
    end as increase_num,
from 
    sign_add3 as t1
left join
    minkabu as t2
    on t1.stock_code = t2.stock_code
);


