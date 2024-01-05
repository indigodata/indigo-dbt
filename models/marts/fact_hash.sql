{{
    config(
        materialized='table'
      , cluster_by=['first_peer']
    )
}}

WITH peer_messages AS (
    SELECT 
        *
      , ARRAY_SIZE(msg_data)    AS msg_hash_count
    FROM {{ source('keystone_offchain', 'network_feed') }}
    WHERE msg_timestamp > sysdate() - interval '1 day'
        AND msg_type IN ('new_hash_66', 'new_hash_68')
)
, peer_hash_msg AS (
    SELECT
        msg.node_id
      , msg.peer_id
      , msg.msg_timestamp
      , msg.msg_type
      , msg.msg_hash_count
      , AS_VARCHAR(hashes.value)    AS tx_hash
      , hashes.index                AS msg_hash_index
    FROM peer_messages msg,
        LATERAL FLATTEN(input => msg_data) hashes
)
, hash_stats_global AS (
    SELECT
        phm.tx_hash                                         AS tx_from
      , ARRAY_AGG(phm.msg_timestamp)
             WITHIN GROUP (ORDER BY msg_timestamp)          AS hash_msg_timestamps
      , ARRAY_AGG(phm.node_id)
            WITHIN GROUP (ORDER BY msg_timestamp)           AS hash_msg_node_order
      , ARRAY_AGG(phm.peer_id) 
            WITHIN GROUP (ORDER BY msg_timestamp)           AS hash_msg_peer_order
    FROM peer_hash_msg phm
    GROUP BY 1
)
SELECT
      AS_VARCHAR(GET(hash_msg_peer_order , 0))              AS first_peer
    , ft.blk_timestamp                                      AS confirmed_at
    , GET(hash_msg_timestamps , 0)                          AS first_seen_at
    , GET(hash_msg_timestamps , 1)                          AS second_seen_at
    , DATEDIFF('nanosecond', first_seen_at, second_seen_at) AS win_margin_nano
    , win_margin_nano / 1000000                             AS win_margin_ms
    , win_margin_nano / 1000000000                          AS win_margin_sec
    , GET(hash_msg_node_order , 0)                          AS first_node
    , GET(hash_msg_node_order , 1)                          AS second_node
    , ARRAY_SIZE(pm.msg_data)                               AS first_msg_hash_count
    , ARRAY_POSITION(hsg.tx_hash::variant, pm.msg_data)     AS first_msg_hash_index
    , hsg.tx_hash
    , hsg.hash_msg_timestamps
    , hsg.hash_msg_node_order
    , hsg.hash_msg_peer_order
    , ARRAY_SIZE(ARRAY_DISTINCT(hash_msg_peer_order))       AS seen_by_count
    , ft.tx_from                                            AS tx_from
FROM hash_stats_global hsg
    INNER JOIN peer_messages pm
        ON pm.msg_timestamp=GET(hash_msg_timestamps , 0)
        AND pm.peer_id=GET(hash_msg_peer_order , 0)
    LEFT JOIN {{ ref('fact_transaction__tx_hash')}} ft
        ON hsg.tx_hash=ft.tx_hash
