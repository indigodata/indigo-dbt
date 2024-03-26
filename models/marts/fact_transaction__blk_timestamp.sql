{{
    config(
        materialized='incremental'
      , cluster_by=['blk_timestamp']
      , tags=['incremental_coordinator']
      , post_hook='{{ update_run_completed() }}'
    )
}}

SELECT
    blk_number
  , blk_timestamp
  , blockchain
  , tx_hash
  , tx_index
  , tx_status
  , tx_from
  , tx_to
  , tx_gas_price
  , tx_gas_used
  , tx_value
  , run_timestamp
FROM {{ ref('stg_keystone__eth_transaction') }}
WHERE {{ new_blocks_only() }}