{{
    config(
        materialized='incremental',
        unique_key = 'log_id',
        tags=['core', 'logs'],
        cluster_by=['block_timestamp']
    )
}}

with

base_txs as (
    select
        *
    from {{ ref("stg_txs") }}
    where {{ incremental_load_filter("ingested_at") }}
),

logs_raw as (
    select
        block_id,
        block_timestamp,
        ingested_at,
        tx_id as tx_hash,
        tx:bech32_from::string as native_origin_from_address,
        tx:bech32_to::string as native_origin_to_address,
        tx:from::string as evm_origin_from_address,
        tx:to::string as evm_origin_to_address,
        tx:receipt:logs as full_logs
    from base_txs
),

logs as (
    select
        block_id,
        block_timestamp,
        ingested_at,
        tx_hash,
        to_number(Right(value:logIndex::string,length(value:logIndex::string)-2), 'xxxxx') as event_index,
        evm_origin_from_address,
        evm_origin_to_address,
        native_origin_from_address,
        native_origin_to_address,
        value:bech32_address::string as native_contract_address,
        value:address::string as evm_contract_address,
        value:decoded:contractName::string as contract_name,
        value:decoded:eventName::string as event_name,
        value:decoded:inputs as event_inputs,
        value:topics as topics,
        value:data::string as data,
        value:removed as event_removed
    from logs_raw,
    lateral flatten ( input => full_logs )
),

final as (
    select
        concat_ws('-', tx_hash, event_index) as log_id,
        *
    from logs
)

select * from final
