{{
    config(
        materialized='incremental'
      , cluster_by=['token_contract']
      , tags=['incremental_coordinator']
      , post_hook='{{ update_run_completed() }}'
    )
}}

WITH erc20_transfer AS (
  SELECT
      blk_number
    , blockchain
    , blk_timestamp
    , tx_hash
    , tx_index
    , tx_from
    , tx_to
    , tx_value
    , call_from
    , log_ordinal
    , token_contract
    , sender
    , recipient
    , amount
    , run_timestamp
  FROM {{ ref('stg_keystone__erc20_transfer') }}

  UNION ALL

  SELECT
      blk_number
    , blockchain
    , blk_timestamp
    , tx_hash
    , tx_index
    , tx_from
    , tx_to
    , tx_value
    , call_from
    , log_ordinal
    , token_contract
    , CASE 
        WHEN transfer_type = 'DEPOSIT'
          THEN transfer_address
        WHEN transfer_type = 'WITHDRAWAL'
          THEN {{ var('zero_address') }}
      END                                 AS sender
    , CASE 
        WHEN transfer_type = 'DEPOSIT'
          THEN {{ var('zero_address') }}
        WHEN transfer_type = 'WITHDRAWAL'
          THEN transfer_address
      END                                 AS recipient
    , amount
    , run_timestamp
  FROM {{ ref('stg_keystone__weth_deposit_withdrawl') }}
)
SELECT
    erc20.blk_number
  , erc20.blockchain
  , erc20.blk_timestamp
  , erc20.tx_hash
  , erc20.tx_index
  , erc20.tx_from
  , erc20.tx_to
  , erc20.tx_value
  , erc20.call_from
  , erc20.log_ordinal
  , erc20.token_contract
  , erc20.sender
  , erc20.recipient
  , erc20.amount
  , erc20.amount / POW(10, price.token_decimals) 
        * (price.start_price_eth + price.end_price_eth) / 2 AS amount_eth
  , erc20.run_timestamp
FROM erc20_transfer erc20
     LEFT JOIN {{ ref('fact_hourly_price_change') }} price
            ON DATE_TRUNC(HOUR, erc20.blk_timestamp) = price.start_hour
                AND erc20.token_contract = price.token_address
WHERE {{ new_blocks_only() }}
