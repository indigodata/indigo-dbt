{{
    config(
        materialized='incremental'
      , cluster_by=['peer_id', 'tx_from']
      , pre_hook=[
            "{% if is_incremental() %}
             SET START_TIMESTAMP = (SELECT MAX(msg_timestamp) FROM {{ this }});
             {% else %}
             SET START_TIMESTAMP = '2024-02-01 00:00:00'::timestamp;
             {% endif %}
             SET END_TIMESTAMP = '2024-02-29 00:00:00'::timestamp;"
        ]
    )
}}

WITH peer_messages AS (
    SELECT 
        *
    FROM {{ source('keystone_offchain', 'network_feed') }}
    -- START DATE
    WHERE msg_timestamp > $START_TIMESTAMP 
        AND msg_timestamp < $END_TIMESTAMP
        AND msg_type IN ('get_tx_66', 'get_tx_68', 'get_tx')
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
