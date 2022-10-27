{{ config(
    materialized = 'view'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['token_address']
    ) }} AS dim_tokens_id,
    token_address,
    token_name,
    token_symbol,
    decimals
FROM
    {{ ref('silver__tokens') }}
