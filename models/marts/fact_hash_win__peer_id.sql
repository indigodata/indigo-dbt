{{
    config(
        materialized='incremental'
      , cluster_by=['first_peer']
      , pre_hook=[
            '{{ func_find_duplicate_indices() }}', 
            '{{ func_remove_indices() }}',
            "{% if is_incremental() %}
             SET START_TIMESTAMP = (SELECT MAX(confirmed_at) FROM {{ this }});
             {% else %}
             SET START_TIMESTAMP = '2024-02-01 00:00:00'::timestamp;
             {% endif %}
             SET END_TIMESTAMP = DATEADD(day, 1, $START_TIMESTAMP);"
        ]
    )
}}

WITH peer_messages AS (
    SELECT 
        *
      , CAST(peer_id AS BINARY(32)) AS peer_id_bin
      , ARRAY_SIZE(msg_data)        AS msg_hash_count
    FROM {{ source('keystone_offchain', 'network_feed') }}
    -- Messages from 10 minutes before first confirmed tx in this run
    WHERE msg_timestamp > $START_TIMESTAMP - INTERVAL '10 minutes'
        AND msg_timestamp <= $END_TIMESTAMP
        AND msg_type IN ('new_hash_66', 'new_hash_68', 'new_hash')
)
, confirmed_txs AS (
    SELECT
        blk_timestamp
      , tx_hash
      , tx_from
    FROM {{ ref('fact_transaction__blk_timestamp') }}
    WHERE blk_timestamp > $START_TIMESTAMP
        AND blk_timestamp <= $END_TIMESTAMP
)
, peer_hash_msg AS (
    SELECT
        msg.node_id
      , msg.peer_id_bin as peer_id
      , msg.msg_timestamp
      , msg.msg_type
      , msg.msg_hash_count
      , CAST(SUBSTRING(hashes.value, 3) AS BINARY(32)) AS tx_hash
      , hashes.index                AS msg_hash_index
    FROM peer_messages msg,
        LATERAL FLATTEN(input => msg_data) hashes
)
, new_hash_messages AS (
    SELECT
        peer_hash_msg.tx_hash
      , ARRAY_AGG(msg_timestamp)
             WITHIN GROUP (ORDER BY msg_timestamp)          AS hash_msg_timestamps
      , ARRAY_AGG(node_id)
            WITHIN GROUP (ORDER BY msg_timestamp)           AS hash_msg_node_order
      , ARRAY_AGG(peer_id) 
            WITHIN GROUP (ORDER BY msg_timestamp)           AS hash_msg_peer_order
    FROM peer_hash_msg
        INNER JOIN confirmed_txs ct
            ON peer_hash_msg.tx_hash=ct.tx_hash
    -- Only include messages that were seen before the transaction was confirmed
    WHERE msg_timestamp BETWEEN blk_timestamp - INTERVAL '10 minutes' AND blk_timestamp 
    GROUP BY 1
)
, hash_messages_enriched AS (
    SELECT
        tx_hash
      , to_binary(GET(hash_msg_peer_order , 0))                           AS first_peer
      , GET(hash_msg_timestamps , 0)                                      AS first_seen_at
      , GET(hash_msg_timestamps , 1)                                      AS second_seen_at
      , GET(hash_msg_node_order , 0)                                      AS first_node
      , GET(hash_msg_node_order , 1)                                      AS second_node
      , ARRAY_SIZE(ARRAY_DISTINCT(hash_msg_peer_order))                   AS seen_by_count
      , find_duplicate_indices(hash_msg_node_order, hash_msg_peer_order)  AS duplicate_indices
      , remove_indices(hash_msg_timestamps, duplicate_indices)            AS msg_timestamps
      , remove_indices(hash_msg_node_order, duplicate_indices)            AS msg_node_order
      , remove_indices(hash_msg_peer_order, duplicate_indices)            AS msg_peer_order
    FROM new_hash_messages
)
SELECT
      hsg.tx_hash
    , pm.peer_id                                                   AS first_peer
    , ct.blk_timestamp                                             AS confirmed_at
    , hsg.first_seen_at::timestamp                                 AS first_seen_at
    , hsg.second_seen_at::timestamp                                AS second_seen_at
    , hsg.first_node
    , hsg.second_node
    , hsg.seen_by_count
    , DATEDIFF('millisecond', first_seen_at, confirmed_at) / 1000  AS confirmation_lag_sec
    , DATEDIFF('nanosecond', first_seen_at, second_seen_at)        AS win_margin_nano
    , win_margin_nano / 1000000000                                 AS win_margin_sec
    , ARRAY_SIZE(pm.msg_data)                                      AS first_msg_hash_count
    , ARRAY_POSITION(hsg.tx_hash::variant, pm.msg_data)            AS first_msg_hash_index
    , hsg.msg_timestamps
    , hsg.msg_node_order
    , hsg.msg_peer_order
    , ct.tx_from
FROM hash_messages_enriched hsg
    INNER JOIN peer_messages pm
        ON pm.msg_timestamp=hsg.first_seen_at
        AND pm.peer_id_bin=hsg.first_peer
    LEFT JOIN confirmed_txs ct
        ON hsg.tx_hash=ct.tx_hash
