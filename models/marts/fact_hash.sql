{{
    config(
        materialized='table'
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
    WHERE msg_timestamp > sysdate() - interval '1 week'
        AND msg_type IN ('new_hash_66', 'new_hash_68')
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
)
, hash_messages AS (
    SELECT
        tx_hash
      , ARRAY_AGG(msg_timestamp)
             WITHIN GROUP (ORDER BY msg_timestamp)          AS hash_msg_timestamps
      , ARRAY_AGG(node_id)
            WITHIN GROUP (ORDER BY msg_timestamp)           AS hash_msg_node_order
      , ARRAY_AGG(peer_id) 
            WITHIN GROUP (ORDER BY msg_timestamp)           AS hash_msg_peer_order
    FROM confirmed_txs
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
    FROM hash_messages
)
SELECT
      hsg.tx_hash
    , pm.peer_id                                            AS first_peer
    , ft.blk_timestamp                                      AS confirmed_at
    , hsg.first_seen_at
    , hsg.second_seen_at
    , hsg.first_node
    , hsg.second_node
    , hsg.seen_by_count
    , DATEDIFF('nanosecond', first_seen_at, second_seen_at) AS win_margin_nano
    , win_margin_nano / 1000000000                          AS win_margin_sec
    , ARRAY_SIZE(pm.msg_data)                               AS first_msg_hash_count
    , ARRAY_POSITION(hsg.tx_hash::variant, pm.msg_data)     AS first_msg_hash_index
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
