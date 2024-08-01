{{
    config(
        materialized='incremental'
    )
}}

WITH uniswap_v3_create_pool_source AS (
    SELECT 
        * 
    FROM {{ source('keystone_ethereum', 'uniswap_v3_create_pool') }}
    WHERE log_type = 'UniswapV3:CreatePool'
    {% if is_incremental() %}
      AND inserted_at > (SELECT MAX(curr.inserted_at) FROM {{ this }} AS curr)
    {% endif %}
)
, uniswap_v3_create_pool_base AS (
    SELECT *
      , ROW_NUMBER() OVER (
          PARTITION BY 
              tx_hash 
            , log_index 
          ORDER BY tx_hash)        AS dup_logs_ct
    FROM uniswap_v3_create_pool_source
)
SELECT 
    blk_number
  , blk_timestamp
  , blk_tx_ct
  , tx_hash
  , tx_index
  , tx_status
  , tx_to
  , tx_from
  , tx_gas_price
  , tx_gas_used
  , tx_value_gwei/POWER(10,9)          AS tx_eth_paid
  , tx_log_ct
  , tx_call_ct
  , log_index
  , log_address                        AS pool_deployer_contract
  , token0
  , token1
  , fee::number                        AS fee
  , tick_spacing::number               AS tick_spacing
  , pool_address
  , inserted_at
FROM uniswap_v3_create_pool_base
WHERE dup_logs_ct=1        -- Remove duplicate rows