{{
    config(
        materialized='table'
    )
}}

WITH uniswap_v3_create_pool AS (
    SELECT *
    FROM {{ ref('stg_keystone__uniswap_v3_create_pool') }}
)
, weth_pools AS (
  SELECT
      blk_number                  AS pool_created_block
    , tx_hash                     AS pool_created_tx_hash
    , blk_timestamp               AS pool_created_at
    , pool_address
    , pool_deployer_contract
    , {{ var('weth_address') }}               AS token0
    , IFF(
        token0 = {{ var('weth_address') }},
        token1,
        token0
      )                                       AS token1
    , fee
    , tick_spacing
    , CASE WHEN token0='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        THEN '0'
        WHEN token1='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        THEN '1'
        ELSE NULL END                           AS weth_token
  FROM uniswap_v3_create_pool pool
  WHERE token0 = {{ var('weth_address') }}
    OR token1 = {{ var('weth_address') }}
)
SELECT 
    pool_created_block
  , pool_created_tx_hash
  , pool_created_at
  , pool_address
  , pool_deployer_contract
  , token0
  , token1
  , 18 AS token0_decimals
  , erc20.decimals AS token1_decimals
  , fee
  , tick_spacing
  , weth_token
FROM weth_pools pool
  LEFT JOIN {{ ref('seed_erc20_decimal') }} erc20
    ON pool.token1 = erc20.token_address
