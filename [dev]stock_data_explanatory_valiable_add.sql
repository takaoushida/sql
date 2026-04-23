DECLARE stock_code STRING;
DECLARE suffix STRING;
--上場廃止銘柄のテーブルがワイルドカードだとメモリオーバーになるので1枚のテーブルにする
for tables in(
    with
    delisting_dataset as(
        select
            distinct
            replace(table_id,'delisting_','') as stock_code
        from    
            stock_data_delisting.__TABLES__
    ),
    delisting_table as(
        select
            distinct 
            stock_code
        from
            stock_data.delisting_tables
    )
    select
        t1.stock_code
    from
        delisting_dataset as t1
    left join
        delisting_table as t2
        on t1.stock_code = t2.stock_code
    where
        t2.stock_code is null and t1.stock_code != 'None'
    order by 1
)
    do  
        execute immediate format(
            """
            insert into stock_data.delisting_tables --日によってフィールドの並び順が違うのでハードコーディング
                select
                    Date,	
                    Open,	
                    High,	
                    Low	,
                    Close,	
                    cast(Volume as int64) as volume,	
                    stock_code
                from
                    `stock_data_delisting.delisting_%s`

            """, 
        tables.stock_code
        );
end for;

create or replace table temp_folder.dev_stock_data_flg_add 
partition by created_at 
cluster by stock_code as(
    with
    delisting_tb as(
        select
            * except(date),
            cast(date as date) as created_at,
        from
            stock_data.delisting_tables
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
            t1.* except(date),
            cast(date as date) as created_at,
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
            * ,
            lead(open,1) over(partition by stock_code order by created_at) as contract_price,--翌日の始値が約定価格
        from
            all_stock_data 
    ),
    up_tb as(
        select
            t1.created_at,
            t1.stock_code,
            min(case when t2.high / t1.contract_price >= 1.1 then t2.created_at end) as up_date,
        from
            base as t1
        left join
            base as t2
            on t1.stock_code = t2.stock_code and t1.created_at between date_add(t2.created_at,interval -120 day) and date_add(t2.created_at,interval -1 day)
        group by 1,2
    ),
    down_tb as(
        select
            t1.created_at,
            t1.stock_code,
            min(case when t2.low / t1.contract_price <= 0.9 then t2.created_at end) as down_date,
        from
            base as t1
        left join
            base as t2
            on t1.stock_code = t2.stock_code and t1.created_at between date_add(t2.created_at,interval -120 day) and date_add(t2.created_at,interval -1 day)
        group by 1,2
    ),
    pre_flg as(
        select
            t1.*,
            up_date,
            down_date,
        from
            base as t1
        left join
            up_tb as t2
            on t1.stock_code = t2.stock_code and t1.created_at = t2.created_at
        left join
            down_tb as t3
            on t1.stock_code = t3.stock_code and t1.created_at = t3.created_at
    )
    select
        * except(up_date,down_date),
        case when up_date is not null then 1 end as up_flg,--モデル学習用のフラグ
        date_diff(up_date,created_at,day) as up_past_day,
        case when down_date is not null then 1 end as down_flg,
        date_diff(down_date,created_at,day) as down_past_day,
        case when up_date < ifnull(down_date,current_date('Asia/Tokyo')) then 1 end as win_flg,--bi上のフラグ
        case when down_date < ifnull(up_date,current_date('Asia/Tokyo')) then 1 end as lose_flg, --2026-03-11修正
    from
        pre_flg
);
##################################################################################################################################################################################################################
##################################################################################################################################################################################################################
create or replace table temp_folder.dev_quartely_report_with_increase_num as
with
quartely_report as(
    select
        t1.* except(earnings_title,operating_income_title,ordinaly_profit_title,net_income_title,ordinaly_profit,before_ordinaly_profit,period_month,omit_flg,xbrl_less_known_flg,stock_reward),
        min(t1.period) over(partition by t1.stock_code) as min_period,
        (t1.net_income - t1.before_net_income)/ nullif(abs(t1.before_net_income),0) as net_income_gain_rate, --前年同期比の純利益増減率
        t1.net_assets / nullif(t1.total_assets,0) as equity_ratio,
        t2.year3_red_count,
        t2.non_cum_stock_reward as stock_reward
    from
        `securities_report.quartely_report_for_learning` as t1
    left join
        `securities_report.quartely_report_for_bi` as t2
        on t1.stock_code = t2.stock_code and t1.period = t2.period and t1.quarter = t2.quarter
),
quartely_report_row_add as(--訂正を含めて最終行を割り出す,stock_code,period,quarter,release_date,refine_flgに対して一意
    select
        *,
        row_number() over(partition by stock_code,period,quarter order by release_date desc,refine_flg desc) as row_number
    from
        quartely_report
),
quartely_report_max_only as(--最終行のみにする 
    select
        * 
    from
        quartely_report_row_add
    where
        row_number = 1
),
quartely_report_lag_add as(--単quarterの純利益を出すために1Q前の純利益を取得
    select
        *,
        lag(net_income) over(partition by stock_code order by release_date) as last_net_income --1Q前の純利益
    from
        quartely_report_max_only
),
--連続増減益年数を集計
quarter4 as(--4期のみにする,irbankをここに加えるつもりだったが証券コードは5年経過すると再利用されるようなので断念
    select
        *,
    from
        quartely_report_lag_add
    where
        quarter = 4
),
irbank as(
    select
        *,
        cast(format_date('%Y',year) as int64) as period
    from
        securities_report.irbank_past_data
),
quarter4_union as(
    select
        coalesce(t1.stock_code,t2.stock_code) as stock_code,
        coalesce(t1.period,t2.period) as period,
        coalesce(t1.earnings,t2.earnings /1000000) as earnings,
        coalesce(t1.operating_income,t2.operating_income /1000000) as operating_income,        
        coalesce(t1.net_income,t2.net_income /1000000) as net_income,
        t1.stock_reward,
    from
        quarter4 as t1
    full outer join
        irbank as t2
        on t1.stock_code = t2.stock_code and t1.period = t2.period
),
quarter4_union_lead_add as(
    select
        *,
        lead(period,1) over(partition by stock_code order by period) as next_period,
    from
        quarter4_union
),
quarter_union_flg_add as(--年度が飛んでいる行にフラグ立て
    select
        *,
        case when next_period != period +1 then 1 end as skip_flg,
    from
        quarter4_union_lead_add
),
quarter4_union_runnings as(
    select
        *,
        sum(skip_flg) over(partition by stock_code order by period desc) as other_code_flg --年が飛んでいる=別銘柄
    from
        quarter_union_flg_add
),
crease_add as(--減益となった場合フラグを立てる
    select
        *,        
        case 
            when earnings / nullif(lag(earnings,1) over(partition by stock_code order by period),0) < 0.95 then 1 
        end as earnings_flg, --売上が前年比95％未満ならフラグ
        case
            when operating_income / nullif(earnings,0) < 0.05 then 1 
        end as operating_income_flg, --営業利益率が5%未満ならフラグ
        case
            when net_income - lag(net_income,1) over(partition by stock_code order by period) < 0 then 1
        end as decrease_flg,
        case
            when net_income - lag(net_income,1) over(partition by stock_code order by period) > 0 then 1
        end as increase_flg,        
    from
        quarter4_union_runnings
    where
        other_code_flg is null --2025-11-13 抜けてたので追加
),
crease_running as(--減益となった累計回数を付与
    select
        *,
        sum(earnings_flg) over(partition by stock_code order by period) as running_earnings_flg,
        sum(operating_income_flg) over(partition by stock_code order by period) as running_operating_income_flg,
        sum(increase_flg) over(partition by stock_code order by period) as running_increase,
        sum(decrease_flg) over(partition by stock_code order by period) as running_decrease
    from
        crease_add
),
crease_num_add as(--累計減益回数毎の累計行数=連続増益回数を集計
    select
        *,
        count(period) over(partition by stock_code,running_earnings_flg order by period) -1 as earnings_num,--売上維持連続期数,increase_numと違いcountなので-1する必要がある
        count(period) over(partition by stock_code,running_operating_income_flg order by period) -1 as operating_income_num,--営業利益率維持連続期数
        sum(increase_flg) over(partition by stock_code,running_decrease order by period) as increase_num,--partition by が逆にしてある点に注意
        sum(decrease_flg) over(partition by stock_code,running_increase order by period) * -1 as decrease_num,--逆にすることで累計が成立する
        max(period) over(partition by stock_code) as max_period
    from
        crease_running
),
crease_num_fin as(
    select
        * except(increase_num,decrease_num),
        coalesce(increase_num,decrease_num) as increase_num
    from
        crease_num_add
),
max_period_only as(--最新の4期のみにする
    select
        *
    from
        crease_num_fin
    where
        period = max_period
),
quartely_report_join_tb as(--最新の期が4期でない場合nullとなってしまうのでその場合最新の4期の連続増益回数を付与,stock_code,period,quarterに対し一意
    select
        t1.* except(min_period,refine_flg),
        case
            when t1.period = t1.min_period and t2.earnings_num is null then null
            else coalesce(t2.earnings_num,t3.earnings_num) 
        end as earnings_num, --4Qを迎えていないperiodに最後の4Qの値を付与
        case
            when t1.period = t1.min_period and t2.operating_income_num is null then null
            else coalesce(t2.operating_income_num,t3.operating_income_num) 
        end as operating_income_num,
        case
            when t1.period = t1.min_period and t2.increase_num is null then null
            else coalesce(t2.increase_num,t3.increase_num) 
        end as increase_num,
        case 
            when t1.quarter = 1 then t1.net_income  --1Qはnet_incomeそのものがquarter_net_income
            else t1.net_income - t1.last_net_income --それ以外は直前のnet_incomeとの差分がquarter_net_income
        end as quarter_net_income,
        case when t2.stock_code is null then 1 end as period_null_flg, --4Qを迎えていないperiodにフラグが立つ
    from
        quartely_report_lag_add as t1
    left join
        crease_num_fin as t2
        on t1.stock_code = t2.stock_code and t1.period = t2.period
    left join
        max_period_only as t3
        on t1.stock_code = t3.stock_code
)
--直近で4Qを迎えていない場合、直近の値で各数値を増減させる
select
    * except(earnings_num,operating_income_num,increase_num,period_null_flg),
    case 
        when period_null_flg is null then earnings_num
        when earnings / nullif(before_earnings,0) >= 0.95 then earnings_num +1 
        else 0
    end as earnings_num,
    case 
        when period_null_flg is null then operating_income_num
        when operating_income / nullif(earnings,0) >= 0.05  then operating_income_num +1 
        else 0
    end as operating_income_num,        
    case 
        when period_null_flg is null then increase_num --4Qを迎えているならそのまま
        --4Qを迎えていない場合
        when increase_num > 0 and net_income - before_net_income >= 0 then increase_num +1 --before_net_incomeは前年同期の純利益,増益中なら前年同期比でプラスならinrease_numが増加
        when increase_num < 0 and net_income - before_net_income < 0 then increase_num -1  --減益中ならマイナスならincrease_numがさらに減る
        when net_income - before_net_income >= 0 then 1 --上2行の逆ケース,増益中で減益になったらとその逆,新たに1 or -1から始まる
        when net_income - before_net_income < 0 then -1
    end as increase_num, --4Qを迎えていない場合最後のincrease_numに値を足す(マイナスなら引く)
    count(release_date) over(partition by stock_code order by release_date desc) as report_num
from
    quartely_report_join_tb;
##################################################################################################################################################################################################################
##################################################################################################################################################################################################################
create or replace table feature_learning_dev.stock_data_explanatory_valiable_add_20260416
partition by created_at 
cluster by stock_code as(
with
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
        temp_folder.dev_stock_data_flg_add  as t1
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
date_tb as(
    select
        distinct
        created_at
    from
        temp_folder.dev_stock_data_flg_add
),
pre_split_tb as(--split_dateが土曜日であることがあるのでsplit_date以降で結合
    select
        t1.* except(split_date),
        t2.created_at,
        row_number() over(partition by stock_code,split_date order by created_at) as split_row_num
    from
        `stock_data.stock_split_*` as t1
    inner join
        date_tb as t2
        on t1.split_date <= t2.created_at
),
split_tb as(--split_date以降の最初の日=翌営業日のみにする
    select
        *,
        created_at as split_date
    from
        pre_split_tb
    where
        split_row_num = 1
),
split_add as(
    select
        t1.*,
        date_diff(t1.created_at,t2.release_date,day) as report_release_past_day,
        t2.* except(stock_code,period),
        (t3.split_stock_amount + t3.exist_stock_amount) / t3.exist_stock_amount as split_rate,--分割率,カテゴリとしては発表日ベース
        t3.release_date as split_release_date,--株式分割発表日
        (t6.split_stock_amount + t6.exist_stock_amount) / t6.exist_stock_amount as real_split_rate,--実際の分割日ベースの分割率
        t4.buyback_flg,
        t5.supervision_reason,
        t5.release_date as supervision_release_date,
        row_number() over(partition by t1.stock_code,t6.split_date order by t1.stock_code) as split_row_num
    from
        temp_folder.dev_stock_data_flg_add as t1
    left join
        temp_folder.dev_quartely_report_with_increase_num as t2
        on t1.stock_code = t2.stock_code and t1.created_at between t2.join_start_date and t2.join_end_date 
    left join
        split_tb as t3
        on t1.stock_code = t3.stock_code and t1.created_at = t3.release_date
    left join
        buyback_unique as t4
        on t1.stock_code = t4.stock_code and t1.created_at = t4.created_at
    left join supervision as t5
        on t1.stock_code = t5.stock_code and t1.created_at between t5.release_date and t5.end_date
    left join
        split_tb as t6
        on t1.stock_code = t6.stock_code and t1.created_at = t6.split_date
),
quartely_report_add as(--実際の分割日ベースの分割率を翌営業日のみにする,split_row_num=1以外には付与されないがjoinで行が増幅するのでdistinct
    select
        distinct
        * except(split_row_num,real_split_rate),
        case when split_row_num = 1 then real_split_rate end as real_split_rate,
    from
        split_add
),
data_tb as(--created_at,stock_codeに対し一意
    select
        *,
        close / nullif(((quarter_net_income*1000000 * 4)/nullif(stock_amount,0)),0) as per,--株価収益率
        close / nullif(((net_assets*1000000)/nullif(stock_amount,0)),0) as pbr,--株価純資産倍率　※2024年版ではnet_assets*1000000*4になっていた,net_assetsは純資産だから四半期ごとの値じゃないので4倍してはいけない
        (quarter_net_income*1000000 * 4) / ((total_assets*1000000) * nullif((equity_ratio),0))  as roe,--自己資本利益率
        (quarter_net_income*1000000 * 4) / nullif((total_assets*1000000),0) as roa,--総資産利益率    
        close * stock_amount as market_cap,--時価総額 
        max(split_release_date) over(partition by stock_code order by created_at) as running_release_date,   
        date_diff(created_at,supervision_release_date,day) as supervision_past_day,
        avg(volume) over(partition by stock_code order by created_at rows between 250 preceding and current row) as avg_volume_1y,  --年間出来高平均      
        EXP(SUM(LOG(IFNULL(real_split_rate,1))) OVER (
            PARTITION BY stock_code
            ORDER BY created_at desc
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS cum_split_rate --利回りを調整後利回りにするための累積値
    from
        quartely_report_add
),
base_aggre as(--各テクニカル指標の元となる値を集計
    select t1.* except(split_rate),
        avg(t1.close) over(partition by t1.stock_code order by t1.created_at rows between 5 preceding and current row) as close_avg1, --5日間平均 
        avg(t1.close) over(partition by t1.stock_code order by t1.created_at rows between 20 preceding and current row) as close_avg2, --20日間平均
        avg(t1.close) over(partition by t1.stock_code order by t1.created_at rows between 60 preceding and current row) as close_avg3, --60日間平均
        lag(t1.close,1) over(partition by t1.stock_code order by t1.created_at) as before_close, --前日の値
        lag(t1.close,2) over(partition by t1.stock_code order by t1.created_at) as before_day3_close, 
        lag(t1.close,3) over(partition by t1.stock_code order by t1.created_at) as before_day4_close, 
        lag(t1.close,4) over(partition by t1.stock_code order by t1.created_at) as before_day5_close, 
        lag(t1.close,5) over(partition by t1.stock_code order by t1.created_at) as before_day6_close, 
        lag(t1.close,6) over(partition by t1.stock_code order by t1.created_at) as before_day7_close, --7日前の値
        lag(t1.close,20) over(partition by t1.stock_code order by t1.created_at) as before_day20_close,
        lag(t1.close,20) over(partition by t1.stock_code order by t1.created_at) as before_day60_close,
        min(t1.close) over(partition by t1.stock_code order by t1.created_at rows between 6 preceding and current row) as range_min,--直近7日間の最安値,
        max(t1.close) over(partition by t1.stock_code order by t1.created_at rows between 6 preceding and current row) as range_max,--直近7日間の最高値,
        min(t1.close) over(partition by t1.stock_code order by t1.created_at rows between 13 preceding and current row) as range_min2,--直近7日間の最安値,
        max(t1.close) over(partition by t1.stock_code order by t1.created_at rows between 13 preceding and current row) as range_max2,--直近7日間の最高値,
        t2.split_rate,
        case 
            when t1.report_num = 1 then
            last_value(t1.real_split_rate ignore nulls) over(partition by t1.stock_code,t1.quarter,t1.join_start_date order by t1.created_at) 
        end as last_split_rate,--直近の株式分割率
        t1.avg_volume_1y / t1.stock_amount as free_float_ratio, --流動株比率
     from 
        data_tb as t1
    left join
        data_tb as t2
        on t1.stock_code = t2.stock_code and t1.running_release_date = t2.split_release_date
),
cum_split_rate_refine as(
    select
        * except(last_split_rate,cum_split_rate,stock_reward),
        stock_reward / greatest(ifnull(cum_split_rate,1),ifnull(last_split_rate,1)) as stock_reward --直近の株式分割率を反映させた調整後配当
    from
        base_aggre 
),
trend_add as(--移動平均のクロスを判別するフラグや各指標の元となる値を引き続き集計
    select * except(split_release_date,running_release_date),
        case when close_avg1 >= close_avg2 then 'upper' else 'lower' end as trend1,--5日間平均が20日平均を上回っていればupper
        case when close_avg2 >= close_avg3 then 'upper' else 'lower' end as trend2,
        case when close > before_close then close - before_close else 0 end as gain,
        case when close < before_close then before_close - close else 0 end as loss,
        min(close) over(partition by stock_code order by created_at rows between 60 preceding and current row) as min_60day_close,--直近N日の最安値
        max(close) over(partition by stock_code order by created_at rows between 60 preceding and current row) as max_60day_close,--直近N日の最安値
        min(close) over(partition by stock_code order by created_at rows between 750 preceding and current row) as min_3year_close,--直近N日の最安値
        max(close) over(partition by stock_code order by created_at rows between 750 preceding and current row) as max_3year_close,--直近N日の最安値
        min(created_at) over(partition by stock_code) min_dt,
        (close - range_min) / nullif((range_max - range_min),0) as k_value,
        (close - range_min2) / nullif((range_max2 - range_min2),0) as k_value2,
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
        (close - before_close) / before_close as daily_volatility, --分母は以前はcloseだった
        lag(stock_reward,1) over(partition by stock_code order by created_at) as before_stock_reward 
    from 
        cum_split_rate_refine
),
trend_lag_add as(--クロス発生か否かを判別するため、前日のフラグ付与,rsiはここで完成
    select 
        *,
        lag(trend1,1) over(partition by stock_code order by created_at) as before_trend1,
        lag(trend2,1) over(partition by stock_code order by created_at) as before_trend2,
        avg(gain) over (partition by stock_code order by created_at rows between 13 preceding and current row) as avg_gain,
        avg(loss) over (partition by stock_code order by created_at rows between 13 preceding and current row) as avg_loss,
        avg(k_value) over(partition by stock_code order by created_at rows between 2 preceding and current row) as d_value,
        avg(k_value2) over(partition by stock_code order by created_at rows between 2 preceding and current row) as d_value2,
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
        case
            when stock_reward / nullif(before_stock_reward,0) > 1 then 1
        end as stock_reward_increase_flg,
    from
        trend_add
),
sign_add as(--前日のフラグと異なるなら売買サイン,stcasticksも移動平均と同様クロス判別が必要なのでここでフラグ建て
    select * except(stock_reward_increase_flg),
        case when trend1 = 'upper' and before_trend1 = 'lower' then 1
             when trend1 = 'lower' and before_trend1 = 'upper' then 4
             when trend1 = 'upper' then 2
             when trend1 = 'lower' then 3
        end as moving_avg,
        case when trend2 = 'upper' and before_trend2 = 'lower' then 1
             when trend2 = 'lower' and before_trend2 = 'upper' then 4
             when trend2 = 'upper' then 2
             when trend2 = 'lower' then 3
        end as moving_avg2,
        case when k_value >= d_value then 'upper' else 'lower' end as stocas_trend,
        case when k_value2 >= d_value2 then 'upper' else 'lower' end as stocas_trend2,
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
        ((up_volume + (stay_volume/2)) / nullif((down_volume + (stay_volume/2)),0)) *100 as volume_ratio,
        sum(stock_reward_increase_flg) over(partition by stock_code order by created_at rows between 4 preceding and current row) as stock_reward_increase_flg,
        case 
            when market_cap >= 500000000000 then 'large'
            when market_cap >= 200000000000 then 'mid'
            else 'small'
        end as market_cap_section,--時価総額
    from  
        trend_lag_add
),
sign_add2 as(
    select 
        *,
        lag(stocas_trend,1) over(partition by stock_code order by created_at) as before_stocas ,
        lag(stocas_trend2,1) over(partition by stock_code order by created_at) as before_stocas2 ,
        ifnull(pow((7- close_rank),2),0) + ifnull(pow((6- day1_close_rank),2),0) + ifnull(pow((5- day2_close_rank),2),0) + ifnull(pow((4- day3_close_rank),2),0) + ifnull(pow((3- day4_close_rank),2),0)
        + ifnull(pow((2- day5_close_rank),2),0) + ifnull(pow((1- day6_close_rank),2),0) as rci_d_value
    from 
        sign_add
),
stock_data_mst_union as(
    select code,type1 from stock_data_mst.stock_data_mst_tokyo_01 union all
    select code,type1 from `stock_data_mst.stock_data_mst_tokyo_delisting_*`
),
stock_data_mst as(
    select
        distinct
        code,
        dense_rank() over(order by type1) as type1
    from
        stock_data_mst_union
),
sign_add3 as(
    select 
        t1.* except(stock_reward_increase_flg),
        case when stocas_trend = 'upper' and before_stocas = 'lower' then 1
             when stocas_trend = 'lower' and before_stocas = 'upper' then 4
             when stocas_trend = 'upper' then 2
             when stocas_trend = 'lower' then 3
        end as stocasticks,
        case when stocas_trend2 = 'upper' and before_stocas2 = 'lower' then 1
             when stocas_trend2 = 'lower' and before_stocas2 = 'upper' then 4
             when stocas_trend2 = 'upper' then 2
             when stocas_trend2 = 'lower' then 3
        end as stocasticks2,
        cast((1 - ((rci_d_value * 6) / (7*48))) * 100 as int64) as rci,  --分母はn(nの2乗-1),7日なので7*48
        close / nullif(close_avg1,0) as short_envelope, 
        close / nullif(close_avg2,0) as envelope, --20日移動平均に対する移動平均乖離率(default)
        close / nullif(close_avg3,0) as long_envelope, 
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
        t2.type1,
        case when t1.stock_reward_increase_flg is not null then 1 end as stock_reward_increase_flg,
        case 
          when t1.report_release_past_day < 7 then 1
          when t1.report_release_past_day < 14 then 2
          when t1.report_release_past_day < 30 then 3
          when t1.report_release_past_day < 60 then 4
          when t1.report_release_past_day < 95 then 5
          else 6 
        end as past_day_tier --決算短信公開後経過日数,増益の場合30日まではup率が高い,減益の場合2か月以上経過すると高い
    from 
        sign_add2 as t1
    left join
        stock_data_mst as t2
        on t1.stock_code = t2.code        
),
minkabu as (
    select
        *
    from
        stock_data_mst.ipo_date_tb
),
point_add as(
    select
        t1.created_at,
        t1.stock_code,
        t1.quarter,
        t1.type1,
        t1.close,
        t1.volume,
        contract_price,--約定値段
        up_flg,
        down_flg,
        win_flg,
        lose_flg,
        stock_reward / nullif(close,0) as reward_rate, --調整後利回り
        stock_reward,--調整後配当
       (net_income - before_net_income) / nullif(abs(before_net_income),0) as quarter_net_income_rate,--純利益(前年同期比)
        per, --株価収益率,株価 ÷ 1株あたり純利益（EPS）,高いほど割高
        pbr,--株価純資産倍率株価 ÷ 1株あたり純資産（BPS）,低いと稼げていない会社,高いと割高
        roe,--自己資本利益率
        roa,--総資産利益率
        moving_avg,--移動平均
        moving_avg2, --2025-12-02追加
        rsi,--14日間のRSI
        stocasticks,--7日間のストキャスティクス
        --k_value,--モデル学習時のみ参照 2025-12-02追加
        --d_value,--モデル学習時のみ参照 2025-12-02追加
        stocasticks2,--14日間のストキャスティクス 2025-12-02追加
        --k_value2,--モデル学習時のみ参照 2025-12-02追加
        --d_value2,--モデル学習時のみ参照 2025-12-02追加
        volume_ratio,
        psychological,
        roc,
        rci,
        short_envelope, --5日間の移動平均乖離率 2025-12-02追加
        envelope, --20日間の移動平均乖離率
        long_envelope, --60日間の移動平均乖離率 2025-12-02追加
        bottom_relative_rate,--直近3年の最安値に対する元終値の割合
        top_relative_rate,--同上の最高値
        day60_bottom_relative_rate,
        day60_top_relative_rate,        
        case 
            when date_diff(t1.created_at,t2.ipo_date,year) < 0 then null --ホールディングスになるなどで再上場の場合再上場日を取得している
            when date_diff(t1.created_at,t2.ipo_date,year) <= 3 then date_diff(t1.created_at,t2.ipo_date,year)
        end as ipo_flg, --上場日から直近1年間にフラグ→直近3年間では実年数
        stock_split,--株式分割(カテゴリ),1:1-1未満,2:1-5未満,3:1-5以上
        buyback_flg,--自社株買い(カテゴリ),買いが開始される20日前～当日にフラグが立つ ※多くの場合は前営業日に発表ではある
        supervision_reason,--管理銘柄になっている理由
        supervision_past_day,
        market_cap_section,--時価総額
        case
            when close_avg2 < 50 then 1
            when close_avg2 < 300 then 2
            when close_avg2 < 1000 then 3
            when close_avg2 < 5000 then 4
            else 5
        end as price_range,--価格帯
        avg(abs(daily_volatility)) over(partition by t1.stock_code order by created_at  rows between 60 preceding and current row) as volatility, --3か月の値動きの荒さ
        close / before_close as day2_crease_rate,
        close / before_day5_close as day5_crease_rate,--前週比
        close / before_day20_close as day20_crease_rate,--前月比
        close / before_day60_close as day60_crease_rate,--三か月前比
        case--元々report_release_past_day<=90で　elseを1としていたがirregular_flgつけたので削除した
            when net_income_gain_rate is null then 2
            when net_income_gain_rate < -5 then 3 -- -500%未満
            when net_income_gain_rate < -1 then 4 -- -100%未満
            when net_income_gain_rate < -0.2 then 5 -- -20%未満
            when net_income_gain_rate < 2 then 6 -- 200%未満
            when net_income_gain_rate >= 2 then 7 --200%未満
        end as net_income_gain_flg,
        case 
            when increase_num <= -3 then -3
            when increase_num >= 3 then 3
            else increase_num
        end as increase_num,
        case 
            when report_release_past_day >= 120 or net_income is null then 1 
        end as irregular_flg,
        change_flg,
        case
            when year3_red_count is null then 0
            when year3_red_count between 0 and 5 then 1
            when year3_red_count between 6 and 11 then 2
            when year3_red_count >= 12 then 3
        end as year3_red_count,--直近3年間の赤字クオーター数　※累積の純利益から算出,四半期ごとに赤字か否かではない
        --earnings_num,--連続売上維持期数
        --operating_income_num,--連続営業利益率維持期数
        --equity_ratio, --自己資本比率
        case 
            when equity_ratio >=0.5 then 2
            when equity_ratio >= 0.3 then 1
            else 0
        end as p1,--自己資本比率による加点
        case
            when roa >= 0.05 then 2
            when roa >= 0.02 then 1
            else 0
        end as p2,--ROA(総資産利益率)による加点
        case
            when roe >= 0.1 then 2
            when roe >= 0.05 then 1
            else 0
        end as p3,--ROE(自己資本利益率)による加点
        case
            when (stock_reward * stock_amount) / nullif(net_income,0) < 0.2 then 1
            when (stock_reward * stock_amount) / nullif(net_income,0) < 0.5 then 2
            when (stock_reward * stock_amount) / nullif(net_income,0) < 0.8 then 1
            else 0
        end as p4,--配当性向による加点
        case
            when operating_income / nullif(earnings,0) >= 0.1 then 2
            when operating_income / nullif(earnings,0) >= 0.05 then 1
            else 0
        end as p5,--営業利益率による加点
        case
            when earnings_num >= 3 then 3
            else ifnull(earnings_num,0)
        end as p6,--連続売上維持期数による加点(最大3)
        case
            when operating_income_num >= 3 then 3
            else ifnull(operating_income_num,0)
        end as p7,--連続営業利益率維持期数による加点(最大3)
        case
            when increase_num >= 3 then 3
            when increase_num < 0 then 0
            else ifnull(increase_num,0)
        end as p8,--連続増益期数による加点(最大3)
        stock_reward_increase_flg, --増配(5日間) 2025-12-02追加
        (quarter_net_income*1000000 * 4) / market_cap as net_income_annualized_ratio, --純利益÷時価総額
        case when (net_income - before_net_income) / nullif(abs(before_net_income),0) > 0 then past_day_tier end as increase_past_day_tier,--増益の場合の決算短信公開後経過日数
        case when (net_income - before_net_income) / nullif(abs(before_net_income),0) < 0 then past_day_tier end as decrease_past_day_tier,--減益の場合の決算短信公開後経過日数
        case 
            when free_float_ratio < 0.002 then 1
            when free_float_ratio < 0.005 then 2
            when free_float_ratio < 0.01 then 3
            when free_float_ratio < 0.02 then 4
            when free_float_ratio >= 0.02 then 5
        end as free_float_ratio_tier,     
        case
            when avg_volume_1y = 0 then 0
            when avg_volume_1y < 1000 then 1
            when avg_volume_1y < 5000 then 2
            when avg_volume_1y < 10000 then 3
            when avg_volume_1y < 50000 then 4
            when avg_volume_1y < 100000 then 5
            when avg_volume_1y >= 100000 then 6
        end as volume_tier,
        case
            when up_past_day < 5 then 1
            when up_past_day < 10 then 2
            when up_past_day < 20 then 3
            when up_past_day < 40 then 4
            when up_past_day < 60 then 5
            when up_past_day < 80 then 6
            when up_past_day < 100 then 7
            when up_past_day >= 100 then 8
        end as up_past_day_tier,
        case
            when down_past_day < 5 then 1
            when down_past_day < 10 then 2
            when down_past_day < 20 then 3
            when down_past_day < 40 then 4
            when down_past_day < 60 then 5
            when down_past_day < 80 then 6
            when down_past_day < 100 then 7
            when down_past_day >= 100 then 8
            else 1
        end as down_past_day_tier,    
        --より数値的にする場合
        case
            when up_past_day < 5 then 5
            when up_past_day >= 90 then 90
        end as cap_up_past_day,
        case
            when down_past_day < 5 then 5
            when down_past_day >= 90 then 90
        end as cap_down_past_day,         
    from 
        sign_add3 as t1
    left join
        minkabu as t2
        on t1.stock_code = t2.stock_code
),
point_sum as(
    select
        * except(p1,p2,p3,p4,p5,p6,p7,p8,net_income_annualized_ratio),
        p1 + p2 + p3 + p4 + p5 + p6 + p7 + p8 as weather_point, --19点満点
        case 
            when net_income_annualized_ratio < -1  then  -1
            when net_income_annualized_ratio >= 1  then 1
            else net_income_annualized_ratio
        end as net_income_annualized_ratio,--純利益÷時価総額
    from
        point_add
),
--市場全体のテクニカル指標作成用サブクエリ
market_base as(
    select
        *,
        (close - before_close) / nullif(before_close,0) as topix_return,--topix用の値動きの割合
        market_cap / nullif(sum(market_cap) over(partition by created_at,market_cap_section),0) as market_cap_rate, --topixを作る際の時価総額帯別加重 
        stddev_pop(daily_volatility) over(partition by stock_code order by created_at rows between 5 preceding and current row) as std_volatility, --銘柄によってボラティリティが異なるため銘柄ごとの標準偏差にする
        stddev_pop(daily_volatility) over(partition by stock_code order by created_at rows between 13 preceding and current row) as std_volatility2
    from
        sign_add3        
),
market_daily as(
    select
        created_at,
        count(stock_code) as ids,
        avg(close / nullif(before_close,0) -1) as daily_return,
        count(case when close - before_close > 0 then stock_code end) as up_ids,
        avg(k_value) as k_value,
        avg(d_value) as d_value,        
        avg(std_volatility) as market_volatility,
        avg(std_volatility2) as market_volatility2,  
        1 - (count(case when up_flg = 1 then stock_code end) / count(stock_code)) as daily_up_weight,  --市況の重み
        1 - (count(case when down_flg = 1 then stock_code end) / count(stock_code)) as daily_down_weight,
    from
        market_base
    group by 1
),
pre_market as(
    select
        *,
        case when k_value >= d_value then 1 end as market_stocasticks,
        avg(daily_return) over(order by created_at rows between 5 preceding and current row) as short_moving_avg, 
        avg(daily_return) over(order by created_at rows between 20 preceding and current row) as long_moving_avg,
        sum(up_ids) over(order by created_at rows between 5 preceding and current row) / sum(ids) over(order by created_at rows between 5 preceding and current row) as market_breath,--上昇銘柄割合(5日平均)
        sum(up_ids) over(order by created_at rows between 13 preceding and current row) / sum(ids) over(order by created_at rows between 13 preceding and current row) as market_breath2,--上昇銘柄割合(14日平均)
        avg(daily_return) over(order by created_at rows between 5 preceding and current row) as market_return,--前日比平均(5日平均)
        avg(daily_return) over(order by created_at rows between 13 preceding and current row) as market_return2,--前日比平均(14日平均)
    from
        market_daily
),
market as(
    select
        *,
        case when short_moving_avg >= long_moving_avg then 1 end as market_moving_avg
    from
        pre_market
),
--時価総額帯別テクニカル指標用のサブクエリ
market_cap_section_daily as(
    select
        created_at,
        market_cap_section,
        avg(close / nullif(before_close,0) -1) as daily_return,
        avg(k_value) as k_value,
        avg(d_value) as d_value,
        sum(topix_return * market_cap_rate) as topix_moving_rate --値動きの割合に加重をかけて合計する
    from
        market_base
    group by 1,2
),
topix_rate_add as(
    select
        *,
        case when k_value >= d_value then 1 end as mcs_stocasticks,
        avg(daily_return) over(partition by market_cap_section order by created_at rows between 5 preceding and current row) as short_moving_avg, 
        avg(daily_return) over(partition by market_cap_section order by created_at rows between 20 preceding and current row) as long_moving_avg,
        EXP(
            SUM(LOG(1 + IFNULL(topix_moving_rate,0))) 
            OVER (
                partition by market_cap_section
                ORDER BY created_at 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS topix_rate --累積にする
    from
        market_cap_section_daily
),
topix_add as(
    select
        * ,
        case when short_moving_avg >= long_moving_avg then 1 end as mcs_moving_avg,
        100 * topix_rate as topix
    from
        topix_rate_add
),
mcs as(
    select
        *,
        topix / min(topix) over(partition by market_cap_section order by created_at rows between 20 preceding and current row) as mcs_bottom_relative_rate,
        topix / max(topix) over(partition by market_cap_section order by created_at rows between 20 preceding and current row) as mcs_top_relative_rate,
    from
        topix_add
),
total_avg as(--学習期間のみで平均をとる
    select
        avg(cap_up_past_day) as avg_cap_up_past_day,
        avg(cap_down_past_day) as avg_cap_down_past_day,
        avg(up_past_day_tier) as avg_up_past_day_tier,
        avg(down_past_day_tier) as avg_down_past_day_tier,        
        count(case when up_flg = 1 then stock_code end) as up_cnt,
        count(case when down_flg = 1 then stock_code end) as down_cnt,
        count(stock_code) as total_ids
    from
        point_add
    where
        created_at between '2016-06-01' and '2024-11-30'
)
select
    t1.* except(weather_point,cap_up_past_day,cap_down_past_day,up_past_day_tier,down_past_day_tier),
    case 
        when weather_point <= 1 then 1 --'thunder'
        when weather_point <= 5 then 2 --'rain'
        when weather_point <= 10 then 3 --'cloudy'
        when weather_point <= 16 then 4 --'partly_cloudy'
        when weather_point >= 17 then 5 --'sun'
    end as weather,
    t2.market_moving_avg, 
    t2.market_stocasticks, 
    t2.market_breath,
    t2.market_breath2, 
    t2.market_return,
    t2.market_return2, 
    t2.market_volatility,
    t2.market_volatility2, 
    t3.mcs_moving_avg, 
    t3.mcs_stocasticks,
    case when t3.market_cap_section = 'small' then  mcs_bottom_relative_rate end as mcs_small_bottom_relative_rate,
    case when t3.market_cap_section = 'mid' then  mcs_bottom_relative_rate end as mcs_mid_bottom_relative_rate,
    case when t3.market_cap_section = 'large' then  mcs_bottom_relative_rate end as mcs_large_bottom_relative_rate,
    case when t3.market_cap_section = 'small' then  mcs_top_relative_rate end as mcs_small_top_relative_rate,
    case when t3.market_cap_section = 'mid' then  mcs_top_relative_rate end as mcs_mid_top_relative_rate,
    case when t3.market_cap_section = 'large' then  mcs_top_relative_rate end as mcs_large_top_relative_rate,
    --市況weight
    daily_up_weight,
    daily_down_weight,
    --所要日数weight
    avg_up_past_day_tier / t1.up_past_day_tier as up_tier_rate,
    avg_down_past_day_tier / t1.down_past_day_tier as down_tier_rate,
    --日数weight×市況
    case
        when t1.up_past_day_tier is null then  daily_up_weight  
        else (avg_up_past_day_tier / t1.up_past_day_tier) * daily_up_weight
    end as up_weight,  
    case
        when t1.down_past_day_tier is null then daily_down_weight
        else (avg_down_past_day_tier / t1.down_past_day_tier) * daily_down_weight
    end as down_weight,   
    --日数weight二乗×市況二乗    
    case
        when t1.up_past_day_tier is null then  power(daily_up_weight,2) 
        else power((avg_up_past_day_tier / t1.up_past_day_tier),2) * power(daily_up_weight,2) --二乗×二乗はweighted_pr_aucが0.98から始まってしまうのでtierでやってみる
    end as up_weight2,
    case
        when t1.down_past_day_tier is null then  power(daily_down_weight,2) 
        else power((avg_down_past_day_tier / t1.down_past_day_tier),2) * power(daily_down_weight,2) --二乗×二乗はweighted_pr_aucが0.98から始まってしまうのでtierでやってみる
    end as down_weight2,
    --日数weight二乗×市況二乗 に上がらなかったときの補正値 0.5 + 上がった行数/全行数:0.56,上がる割合が6％多いから上がらなかった行は6%分重みを増やす    
    case
        when t1.up_past_day_tier is null then  power(daily_up_weight,2) * (0.5 + (up_cnt / total_ids))
        else power((avg_up_past_day_tier / t1.up_past_day_tier),2) * power(daily_up_weight,2) --二乗×二乗はweighted_pr_aucが0.98から始まってしまうのでtierでやってみる
    end as up_weight3,
    case
        when t1.down_past_day_tier is null then  power(daily_up_weight,2) * (0.5 + (down_cnt / total_ids))
        else power((avg_down_past_day_tier / t1.down_past_day_tier),2) * power(daily_down_weight,2) --二乗×二乗はweighted_pr_aucが0.98から始まってしまうのでtierでやってみる
    end as down_weight3,
    --日数weight二乗×市況二乗 に上がらなかったときの補正値 上がった行数/全行数:0.56,本来はイーブン0.5、だから0.56/0.5分重みを増やす  
    case
        when t1.up_past_day_tier is null then  power(daily_up_weight,2) * ((up_cnt / total_ids)/0.5)
        else power((avg_up_past_day_tier / t1.up_past_day_tier),2) * power(daily_up_weight,2) --二乗×二乗はweighted_pr_aucが0.98から始まってしまうのでtierでやってみる
    end as up_weight4,
    case
        when t1.down_past_day_tier is null then  power(daily_down_weight,2) * ((down_cnt / total_ids)/0.5)
        else power((avg_down_past_day_tier / t1.down_past_day_tier),2) * power(daily_down_weight,2) --二乗×二乗はweighted_pr_aucが0.98から始まってしまうのでtierでやってみる
    end as down_weight4,
    --+
    
    case
        when t1.up_past_day_tier is null then  power(daily_up_weight,2) * (up_cnt / (total_ids - up_cnt))
        else power((avg_up_past_day_tier / t1.up_past_day_tier),2) * power(daily_up_weight,2) --二乗×二乗はweighted_pr_aucが0.98から始まってしまうのでtierでやってみる
    end as up_weight5,
    case
        when t1.down_past_day_tier is null then  power(daily_down_weight,2) * (down_cnt / (total_ids - down_cnt))
        else power((avg_down_past_day_tier / t1.down_past_day_tier),2) * power(daily_down_weight,2) --二乗×二乗はweighted_pr_aucが0.98から始まってしまうのでtierでやってみる
    end as down_weight5
from
    point_sum as t1
left join
    market as t2
    on t1.created_at = t2.created_at
left join
    mcs as t3
    on t1.created_at = t3.created_at and t1.market_cap_section = t3.market_cap_section
cross join
    total_avg
);

