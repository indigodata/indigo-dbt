{{ config(materialized='ephemeral') }}

WITH weth_deposit_withdrawl AS (
    SELECT 
        *
      , 'ethereum' AS blockchain
    FROM {{ source('keystone_ethereum', 'weth_deposit_withdrawal') }}
    {% if incremental_models_in_selector() %}
      WHERE blk_number >= {{ get_run_blocks('ethereum', 'run_start_blk_number') }}
        AND blk_number <= {{ get_run_blocks('ethereum', 'run_stop_blk_number') }}
    {% endif %}
)
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
  , log_address                           AS token_contract
  , log_type                              AS transfer_type
  , log_eth_address                       AS transfer_address
  , log_weth_value::NUMBER                AS amount
  , '{{run_started_at}}'::timestamp_ntz   AS run_timestamp
FROM weth_deposit_withdrawl
