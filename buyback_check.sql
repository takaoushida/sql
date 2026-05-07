select
  *,
  count(release_date) over(partition by stock_code,release_date,end_date,buyback_amount) as cnt
from
  `stock_data.buyback_*`
qualify cnt != 1
