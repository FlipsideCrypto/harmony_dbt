{{ config(
  materialized = 'incremental',
  unique_key = "key",
  incremental_strategy = 'delete+insert',
  tags = ['core', 'defi', 'tokenprice'],
  cluster_by = ['block_date', 'token_address']
) }}

WITH stables AS (

  SELECT
    token_address,
    token_name,
    token_symbol,
    decimals
  FROM
    {{ ref('silver__harmony_stable_tokens') }}
),
swaps_ref AS (
  SELECT
    *
  FROM
    {{ ref('silver__swaps') }}
  WHERE
    {{ incremental_last_x_days(
      "block_timestamp",
      3
    ) }}
),
-- trim the swaps table, truncate to day, and normalize the decimals, remove nulls with inner join
simpleswaps AS (
  SELECT
    DATE_TRUNC(
      'day',
      s.block_timestamp
    ) AS block_date,
    s.tx_hash,
    s.pool_address,
    s.token0_address,
    s.token0_symbol,
    s.token1_address,
    s.token1_symbol,
    (
      s.amount0in + s.amount0out
    ) / pow(
      10,
      t0.decimals
    ) AS amt0,
    (
      s.amount1in + s.amount1out
    ) / pow(
      10,
      t1.decimals
    ) AS amt1
  FROM
    swaps_ref AS s
    INNER JOIN {{ ref('silver__tokens') }} AS t0
    ON t0.token_address = s.token0_address
    INNER JOIN {{ ref('silver__tokens') }} AS t1
    ON t1.token_address = s.token1_address
),
-- aggregate daily swap volumes between all pairs
swaps_daily_agg AS (
  SELECT
    s.block_date,
    s.pool_address,
    s.token0_address,
    s.token0_symbol,
    s.token1_address,
    s.token1_symbol,
    SUM(
      s.amt0
    ) AS amt0,
    SUM(
      s.amt1
    ) AS amt1
  FROM
    simpleswaps AS s
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6
),
-- union all 0->1 and 1->0 swaps into a tall table for simplified bi-directional lookups and volume ranking
consolidated_pairs AS (
  SELECT
    concat_ws(
      '-',
      block_date,
      token_address
    ) AS key,
    block_date,
    token_address,
    token_symbol,
    SUM(amt) AS amt,
    pair_address,
    pair_symbol,
    SUM(pair_amt) AS pair_amt,
    SUM(pair_amt) / SUM (amt) AS price,
    ROW_NUMBER() over (
      PARTITION BY block_date,
      token_address
      ORDER BY
        SUM(amt) DESC
    ) AS RANK -- can't use rank() because of ties for 1st
  FROM
    (
      SELECT
        block_date,
        token0_address AS token_address,
        token0_symbol AS token_symbol,
        amt0 AS amt,
        token1_address AS pair_address,
        token1_symbol AS pair_symbol,
        amt1 AS pair_amt
      FROM
        swaps_daily_agg
      UNION ALL
      SELECT
        block_date,
        token1_address AS token_address,
        token1_symbol AS token_symbol,
        amt1 AS amt,
        token0_address AS pair_address,
        token0_symbol AS pair_symbol,
        amt0 AS pair_amt
      FROM
        swaps_daily_agg
    )
  GROUP BY
    1,
    2,
    3,
    4,
    6,
    7
  ORDER BY
    4,
    2,
    5 DESC
),
start_stables AS (
  SELECT
    DISTINCT concat_ws(
      '-',
      C.block_date,
      C.token_address
    ) AS key,
    C.block_date,
    s.token_address,
    s.token_symbol,
    1 AS usd_price,
    0 AS volume_for_price,
    NULL AS price_pair_token,
    NULL AS price_pair_symbol,
    'stables' AS lookup_round
  FROM
    consolidated_pairs AS C
    INNER JOIN stables AS s
    ON C.token_address = s.token_address
),
-- add WONE lookup
wone_lookup AS (
  SELECT
    C.key,
    C.block_date,
    C.token_address,
    C.token_symbol,
    SUM(
      C.pair_amt
    ) / SUM(
      C.amt
    ) AS usd_price,
    SUM(
      C.amt
    ) AS volume_for_price,
    NULL AS price_pair_token,
    NULL AS price_pair_symbol,
    'wONE' AS lookup_round
  FROM
    consolidated_pairs AS C
    INNER JOIN start_stables AS s
    ON C.pair_address = s.token_address
  WHERE
    C.token_address = '0xcf664087a5bb0237a0bad6742852ec6c8d69a27a' -- WONE
  GROUP BY
    1,
    2,
    3,
    4,
    7,
    8
  UNION ALL
  SELECT
    *
  FROM
    start_stables
),
-- add lookups1a table for next round of matches, where top pair matches the wone_lookup table (wone + stables) (exclude keys that already exist in wone_lookup_table)
lookups1a AS (
  SELECT
    C.key,
    C.block_date,
    C.token_address,
    C.token_symbol,
    C.price * s.usd_price AS usd_price,
    C.amt AS volume_for_price,
    C.pair_address AS pair_token_for_price,
    C.pair_symbol AS pair_symbol_for_price,
    '1a' AS lookup_round
  FROM
    consolidated_pairs AS C
    INNER JOIN wone_lookup AS s
    ON C.pair_address = s.token_address
    AND C.block_date = s.block_date
    AND C.rank = 1
  WHERE
    C.key NOT IN (
      SELECT
        key
      FROM
        wone_lookup
    )
  UNION ALL
  SELECT
    *
  FROM
    wone_lookup
),
-- add lookups1b table for next round of matches, where top pair matches the lookups1a table (exclude keys that already exist in lookups1a table)
lookups1b AS (
  SELECT
    C.key,
    C.block_date,
    C.token_address,
    C.token_symbol,
    C.price * s.usd_price AS usd_price,
    C.amt AS volume_for_price,
    C.pair_address AS pair_token_for_price,
    C.pair_symbol AS pair_symbol_for_price,
    '1b' AS lookup_round
  FROM
    consolidated_pairs AS C
    INNER JOIN lookups1a AS s
    ON C.pair_address = s.token_address
    AND C.block_date = s.block_date
    AND C.rank = 1
  WHERE
    C.key NOT IN (
      SELECT
        key
      FROM
        lookups1a
    )
  UNION ALL
  SELECT
    *
  FROM
    lookups1a
),
-- add lookups1c table for next round of matches, where top pair matches the lookups1b table (exclude keys that already exist in lookups1b table)
lookups1c AS (
  SELECT
    C.key,
    C.block_date,
    C.token_address,
    C.token_symbol,
    C.price * s.usd_price AS usd_price,
    C.amt AS volume_for_price,
    C.pair_address AS pair_token_for_price,
    C.pair_symbol AS pair_symbol_for_price,
    '1c' AS lookup_round
  FROM
    consolidated_pairs AS C
    INNER JOIN lookups1b AS s
    ON C.pair_address = s.token_address
    AND C.block_date = s.block_date
    AND C.rank = 1
  WHERE
    C.key NOT IN (
      SELECT
        key
      FROM
        lookups1b
    )
  UNION ALL
  SELECT
    *
  FROM
    lookups1b
),
-- add lookups2a table for next round of matches, where 2nd most common pair matches the lookups1c table (exclude keys that already exist in lookups1c table)
lookups2a AS (
  SELECT
    C.key,
    C.block_date,
    C.token_address,
    C.token_symbol,
    C.price * s.usd_price AS usd_price,
    C.amt AS volume_for_price,
    C.pair_address AS pair_token_for_price,
    C.pair_symbol AS pair_symbol_for_price,
    '2a' AS lookup_round
  FROM
    consolidated_pairs AS C
    INNER JOIN lookups1c AS s
    ON C.pair_address = s.token_address
    AND C.block_date = s.block_date
    AND C.rank = 2
  WHERE
    C.key NOT IN (
      SELECT
        key
      FROM
        lookups1c
    )
  UNION ALL
  SELECT
    *
  FROM
    lookups1c
),
-- add lookups2b table for next round of matches, where 2nd most common pair matches the lookups2a table (exclude keys that already exist in lookups2a table)
lookups2b AS (
  SELECT
    C.key,
    C.block_date,
    C.token_address,
    C.token_symbol,
    C.price * s.usd_price AS usd_price,
    C.amt AS volume_for_price,
    C.pair_address AS pair_token_for_price,
    C.pair_symbol AS pair_symbol_for_price,
    '2b' AS lookup_round
  FROM
    consolidated_pairs AS C
    INNER JOIN lookups2a AS s
    ON C.pair_address = s.token_address
    AND C.block_date = s.block_date
    AND C.rank = 1
  WHERE
    C.key NOT IN (
      SELECT
        key
      FROM
        lookups2a
    )
  UNION ALL
  SELECT
    *
  FROM
    lookups2a
) -- IF needed, additional lookup rounds could be added, e.g.:
-- TODO: add lookups2c table for next round of matches, where 2nd most common pair matches the lookups2b table (exclude keys that already exist in lookups2b table)
-- TODO: add lookups3a table for next round of matches, where 3rd most common pair matches the lookups2c table (exclude keys that already exist in lookups2c table)
-- TODO: add lookups3b table for next round of matches, where 3rd most common pair matches the lookups3a table (exclude keys that already exist in lookups3a table)
-- TODO: add lookups3c table for next round of matches, where 3rd most common pair matches the lookups3b table (exclude keys that already exist in lookups3b table)
SELECT
  l.block_date,
  l.token_address,
  l.token_symbol,
  l.usd_price,
  l.usd_price * SUM(
    C.amt
  ) AS usd_volume,
  SUM(
    C.amt
  ) AS token_volume,
  l.pair_token_for_price,
  l.pair_symbol_for_price,
  l.volume_for_price,
  l.key,
  l.lookup_round
FROM
  lookups2b AS l
  LEFT JOIN consolidated_pairs AS C
  ON l.key = C.key
GROUP BY
  l.block_date,
  l.token_address,
  l.token_symbol,
  l.usd_price,
  l.pair_token_for_price,
  l.pair_symbol_for_price,
  l.volume_for_price,
  l.key,
  l.lookup_round
