{{
    config(
        materialized='incremental',
        unique_key='log_id',
        incremental_strategy = 'delete+insert',
        tags=['core', 'swaps'],
        cluster_by=['block_timestamp']
        )
}}

with

logs as (
    select 
        *
    from {{ ref('logs') }}
    where {{ incremental_load_filter("ingested_at") }}
),

final as (
    select 
        l.block_timestamp,
        l.block_id,
        l.ingested_at,
        l.log_id,
        l.tx_hash,
        l.evm_contract_address as pool_address,
        l.event_index,
        l.evm_origin_from_address,
        l.evm_origin_to_address,
        l.native_origin_from_address,
        l.native_origin_to_address,
        p.token0 as token0_address,
        t0.token_name as token0_name,
        t0.token_symbol as token0_symbol,
        TRY_TO_NUMBER(
            l.event_inputs :amount0In :: STRING
        ) AS amount0In,
        TRY_TO_NUMBER(
            l.event_inputs :amount1In :: STRING
        ) AS amount1In,
        TRY_TO_NUMBER(
            l.event_inputs :amount0Out :: STRING
        ) AS amount0Out,
        TRY_TO_NUMBER(
            l.event_inputs :amount1Out :: STRING
        ) AS amount1Out,
        p.token1 as token1_address,
        t1.token_name as token1_name,
        t1.token_symbol as token1_symbol,
        l.event_inputs:sender::string as from_address,
        l.event_inputs:to as to_address
    from logs as l
    join {{ ref('liquidity_pools') }} as p on p.pool_address = l.evm_contract_address
    left join {{ ref('tokens') }} as t0 on t0.token_address = p.token0
    left join {{ ref('tokens') }} as t1 on t1.token_address = p.token1
    where l.event_name = 'Swap'
)

select * from final
