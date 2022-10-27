{{ config(
  materialized = 'view'
) }}

SELECT
  {{ dbt_utils.surrogate_key(
    ['key']
  ) }} AS fact_tokenprices_id,
  block_date,
  token_address,
  token_symbol,
  usd_price,
  usd_volume,
  token_volume
FROM
  {{ ref('silver__tokenprices') }}
