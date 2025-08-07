DECLARE before_suffix STRING;
DECLARE after_suffix  STRING;
DECLARE delisting_count INT64;

-- 最新と1つ前のsuffixを取得
SET (after_suffix, before_suffix) = (
  SELECT AS STRUCT
    ARRAY_AGG(suffix_date ORDER BY CAST(suffix_date AS INT64) DESC)[OFFSET(0)] AS after_suffix,
    ARRAY_AGG(suffix_date ORDER BY CAST(suffix_date AS INT64) DESC)[OFFSET(1)] AS before_suffix
  FROM (
    SELECT DISTINCT _TABLE_SUFFIX AS suffix_date
    FROM `stock_data.tokyo_01_*`
  )
);

-- delisting候補の件数を事前チェック
SET delisting_count = (
  WITH
    before_tb AS (
      SELECT DISTINCT stock_code
      FROM `stock_data.tokyo_01_*`
      WHERE _TABLE_SUFFIX = before_suffix
    ),
    after_tb AS (
      SELECT DISTINCT stock_code
      FROM `stock_data.tokyo_01_*`
      WHERE _TABLE_SUFFIX = after_suffix
    ),
    delisting_code AS (
      SELECT b.stock_code
      FROM before_tb b
      LEFT JOIN after_tb a USING (stock_code)
      WHERE a.stock_code IS NULL
    )
  SELECT COUNT(*) FROM delisting_code
);

-- 件数が0でなければテーブルを作成
IF delisting_count > 0 THEN
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TABLE `stock_data.delisting_%s` AS
    WITH
        before_tb AS (
          SELECT DISTINCT stock_code
          FROM `stock_data.tokyo_01_*`
          WHERE _TABLE_SUFFIX = '%s'
        ),
        after_tb AS (
          SELECT DISTINCT stock_code
          FROM `stock_data.tokyo_01_*`
          WHERE _TABLE_SUFFIX = '%s'
        ),
        delisting_code AS (
          SELECT t1.stock_code
          FROM before_tb AS t1
          LEFT JOIN after_tb AS t2
          ON t1.stock_code = t2.stock_code
          WHERE t2.stock_code IS NULL
        )
    SELECT t1.*
    FROM `stock_data.tokyo_01_*` AS t1
    INNER JOIN delisting_code AS t2
    ON t1.stock_code = t2.stock_code
    WHERE t1._TABLE_SUFFIX = '%s'
  """, before_suffix, before_suffix, after_suffix, before_suffix);
END IF;
