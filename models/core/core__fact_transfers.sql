{{ config(
    materialized = 'view'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['log_id']
    ) }} AS fact_transfers_id,
    log_id,
    block_id,
    tx_hash,
    block_timestamp,
    contract_address,
    from_address,
    to_address,
    raw_amount
FROM
    {{ ref('silver__transfers') }}
