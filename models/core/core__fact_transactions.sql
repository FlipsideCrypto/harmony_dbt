{{ config(
    materialized = 'view'
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['tx_hash']
    ) }} AS fact_transactions_id,
    block_timestamp,
    nonce,
    INDEX,
    native_from_address,
    native_to_address,
    from_address,
    to_address,
    VALUE,
    block_number,
    block_hash,
    gas_price,
    gas,
    tx_hash,
    DATA,
    status
FROM
    {{ ref('silver__transactions') }}
