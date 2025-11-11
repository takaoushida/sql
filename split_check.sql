select
  stock_code,
  split_date,
  count(release_date) over(partition by stock_code,split_date) as cnt
from
  `stock_data.stock_split_*`
qualify cnt != 1
