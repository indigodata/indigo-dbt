{{
    config(
          materialized='incremental'
        , cluster_by=['pool_address']
    )
}}

WITH uniswap_v3_pair_swap AS (
    SELECT *
    FROM {{ ref('stg_keystone__uniswap_v3_pair_swap') }}
    {% if is_incremental() %}
      WHERE inserted_at > (SELECT MAX(curr.inserted_at) FROM {{ this }} AS curr)
    {% endif %}
)
, liquidity_pool AS (
    SELECT
        * 
    FROM {{ ref('dim_liquidity_pool') }}
)
SELECT 
    swap.*
      EXCLUDE (amount0, amount1)
  , IFF(lp.weth_token = 0,
      swap.amount0,
      swap.amount1
    )                       AS amount0
  , IFF(lp.weth_token = 1,
      swap.amount0,
      swap.amount1
    )                       AS amount1
  , lp.token0
  , lp.token1
  , lp.fee
  , lp.pool_created_block
  , lp.pool_created_at
FROM uniswap_v3_pair_swap swap
    INNER JOIN liquidity_pool lp 
        ON swap.pool_address = lp.pool_address
