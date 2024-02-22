{{
    config(
        materialized='incremental'
      , cluster_by=['peer_id', 'tx_from']
    )
}}

WITH peer_messages AS (
    SELECT 
        *
    FROM {{ source('keystone_offchain', 'network_feed') }}
    -- START DATE
    WHERE msg_timestamp > '2024-01-22 00:00:00'
        AND msg_timestamp < '2024-01-23 00:00:00'
        AND msg_type = 'get_tx_66'
    {% if is_incremental() %}
        AND msg_timestamp > (SELECT MAX(msg_timestamp) FROM {{ this }})
    {% endif %}
)
, peer_hash_msg AS (
    SELECT
        msg.peer_id
      , CAST(SUBSTRING(hashes.value, 3) AS BINARY(32)) AS tx_hash
      , MIN(msg.msg_timestamp)                         AS msg_timestamp
    FROM peer_messages msg,
        LATERAL FLATTEN(input => msg_data) hashes
    GROUP BY 1, 2
)
SELECT
    phm.peer_id
  , ft.tx_from
  , phm.tx_hash
  , phm.msg_timestamp
  , ft.blk_timestamp
FROM peer_hash_msg phm
    INNER JOIN {{ ref('fact_transaction__tx_hash') }} ft
        ON phm.tx_hash=ft.tx_hash
        AND phm.msg_timestamp < ft.blk_timestamp + INTERVAL '1 minute'
    {% if is_incremental() %}
        LEFT JOIN {{ this }} this
            ON phm.peer_id = this.peer_id
            AND ft.tx_from = this.tx_from
            AND phm.tx_hash = this.tx_hash
        WHERE this.tx_hash IS NULL
    {% endif %}
