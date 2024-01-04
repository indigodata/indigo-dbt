{{ config(materialized='ephemeral') }}

WITH eth_transaction_base AS (
    SELECT 
        *
      , 'ethereum' AS blockchain
    FROM {{ source('keystone_ethereum', 'eth_transaction') }}
    {% if incremental_models_in_selector() %}
      WHERE blk_number >= {{ get_run_blocks('ethereum', 'run_start_blk_number') }}
        AND blk_number <= {{ get_run_blocks('ethereum', 'run_stop_blk_number') }}
    {% endif %}
)

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
    , '{{run_started_at}}'::timestamp_ntz  AS run_timestamp
FROM eth_transaction_base
