{{
    config(
        materialized='incremental'
      , cluster_by=['first_peer']
      , pre_hook=[
            '{{ func_find_duplicate_indices() }}', 
            '{{ func_remove_indices() }}'
        ]
    )
}}

WITH peer_messages AS (
    SELECT 
        *
      , CAST(peer_id AS BINARY(32)) AS peer_id_bin
      , ARRAY_SIZE(msg_data)    AS msg_hash_count
    FROM {{ source('keystone_offchain', 'network_feed') }}
    -- START DATE
    WHERE msg_timestamp > '2024-01-22 00:00:00'
    {% if is_incremental() %}
        -- 10 minutes before last run
        AND msg_timestamp > (SELECT MAX(first_seen_at)::timestamp - INTERVAL '10 minutes' FROM {{ this }})
        -- Max data in a single run (1 day)
        AND msg_timestamp < (SELECT MAX(first_seen_at)::timestamp + INTERVAL '1 day' FROM {{ this }})
    {% else %}
        -- LIMIT FULL REFRESH TO 1 DAY
        AND msg_timestamp < '2024-01-23 00:00:00'
    {% endif %}
        AND msg_type IN ('new_hash_66', 'new_hash_68', 'get_tx_66')
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
, confirmed_txs AS (
    SELECT
        phm.*
    FROM peer_hash_msg phm
     INNER JOIN {{ ref('fact_transaction__tx_hash') }} ft
        ON phm.tx_hash=ft.tx_hash
    WHERE phm.msg_timestamp < ft.blk_timestamp
        -- Cut messages that come in more than 10 mins before the tx is confirmed. 
        -- This gets rid of duplicates on the edge of incremental runs 
        -- at the cost of missing hash wins for transactions that take more than 10 mins to confirm.
        AND phm.msg_timestamp > ft.blk_timestamp - INTERVAL '10 minutes'
        -- Only include transactions that were confirmed before the latest message
        AND ft.blk_timestamp <= (SELECT MAX(msg_timestamp) - INTERVAL '10 minutes' FROM peer_messages)
    {% if is_incremental() %}
        -- Only include transactions confirmed after the newest tx in the table
        AND ft.blk_timestamp > (SELECT MAX(confirmed_at)::timestamp FROM {{ this }})
    {% endif %}
)
, new_hash_messages AS (
    SELECT
        tx_hash
      , ARRAY_AGG(msg_timestamp)
             WITHIN GROUP (ORDER BY msg_timestamp)          AS hash_msg_timestamps
      , ARRAY_AGG(node_id)
            WITHIN GROUP (ORDER BY msg_timestamp)           AS hash_msg_node_order
      , ARRAY_AGG(peer_id) 
            WITHIN GROUP (ORDER BY msg_timestamp)           AS hash_msg_peer_order
    FROM confirmed_txs
    WHERE msg_type IN ('new_hash_66', 'new_hash_68')
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
    , ft.blk_timestamp                                             AS confirmed_at
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
    , ft.tx_from
FROM hash_messages_enriched hsg
    INNER JOIN peer_messages pm
        ON pm.msg_timestamp=hsg.first_seen_at
        AND pm.peer_id_bin=hsg.first_peer
    LEFT JOIN {{ ref('fact_transaction__tx_hash')}} ft
        ON hsg.tx_hash=ft.tx_hash
