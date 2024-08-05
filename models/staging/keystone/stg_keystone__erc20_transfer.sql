{{ config(materialized='ephemeral') }}

WITH erc20_transfer_base AS (
    SELECT 
        *
      , 'ethereum' AS blockchain
    FROM {{ source('keystone_ethereum', 'erc20_transfer') }}
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
  , log_from                              AS sender
  , log_to                                AS recipient
  , log_value::NUMBER                     AS amount
  , '{{run_started_at}}'::timestamp_ntz   AS run_timestamp
FROM erc20_transfer_base
WHERE LENGTH(log_value) < 39
    -- OMIT ERC20 transfers that are too large (MAX 38 digits)
