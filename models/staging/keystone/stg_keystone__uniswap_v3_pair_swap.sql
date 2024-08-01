{{
    config(
        materialized='incremental'
    )
}}

WITH uniswap_v3_pair_swap_source AS (
    SELECT 
        * 
    FROM {{ source('keystone_ethereum', 'uniswap_v3_pair_swap') }}
    WHERE log_type = 'UniswapV3Pair:Swap'
			-- OMIT swap ammount that are too large (MAX 38 digits)
      AND LEN(log_field_3) < 39
      AND LEN(log_field_4) < 39
    {% if is_incremental() %}
      AND inserted_at > (SELECT MAX(curr.inserted_at) FROM {{ this }} AS curr)
    {% endif %}
)
, uniswap_v3_pair_swap_base AS (
    SELECT *
      , ROW_NUMBER() OVER (
          PARTITION BY 
              tx_hash 
            , log_index 
          ORDER BY tx_hash)        AS dup_logs_ct
    FROM uniswap_v3_pair_swap_source
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
  , log_address                        AS pool_address
  , log_field_1                        AS sender
  , log_field_2                        AS recipient
  , log_field_3::number                AS amount0
  , log_field_4::number                AS amount1
  , log_field_5                        AS sqrt_price
  , log_field_6::number                AS liquidity
  , log_field_7::number                AS tick
  , inserted_at
FROM uniswap_v3_pair_swap_base
WHERE dup_logs_ct=1        -- Remove duplicate rows