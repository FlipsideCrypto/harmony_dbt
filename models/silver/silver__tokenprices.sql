{{ config(
  materialized = 'incremental',
  unique_key = "key",
  incremental_strategy = 'delete+insert',
  tags = ['core', 'defi', 'tokenprice'],
  cluster_by = ['block_date', 'token_address']
) }}

SELECT
  key,
  block_date,
  token_address,
  token_symbol,
  usd_price,
  usd_volume,
  token_volume
FROM
  {{ ref('silver__tokenprice_from_swaps') }}
WHERE
  {{ incremental_last_x_days(
    "block_date",
    3
  ) }}
  AND usd_volume >= 100
