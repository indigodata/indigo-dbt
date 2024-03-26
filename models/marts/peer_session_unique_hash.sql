{{
    config(
        materialized='table'
      , cluster_by=['peer_id']
    )
}}

with
metadata as (
    select
        min(msg_timestamp) as start_time,
        max(msg_timestamp) as end_time,
        floor(date_part(epoch_second, start_time) / 60) as bin_num_start,
        floor(date_part(epoch_second, end_time) / 60) as bin_num_end
    from KEYSTONE_OFFCHAIN.NETWORK_FEED
    WHERE msg_timestamp BETWEEN '2024-01-22' AND '2024-01-29'
        AND msg_type IN ('new_hash', 'new_hash_66', 'new_hash_68')
)
, bins_base as (
    select
        seq4() as row_num
    from table(generator(rowcount => 1e9))
)
, bins as (
    select
        bins_base.row_num as bin_num
    from bins_base
    inner join metadata
        on bins_base.row_num between metadata.bin_num_start and metadata.bin_num_end
)
, peer_messages as (
    select
          *
        , floor(date_part(epoch_second, MSG_TIMESTAMP) / 60) as bin_num
        , CAST(SUBSTRING(hashes.value, 3) AS BINARY(32)) AS tx_hash
    from KEYSTONE_OFFCHAIN.NETWORK_FEED,
        LATERAL FLATTEN(input => msg_data) hashes
    WHERE msg_timestamp BETWEEN '2024-01-22' AND '2024-01-29'
        AND msg_type IN ('new_hash', 'new_hash_66', 'new_hash_68')
)
, confirmed as (
    SELECT
          msg.peer_id
        , msg.node_id
        , msg_timestamp
        , msg.bin_num
        , ft.tx_hash
        , ft.tx_hash is not null AS is_sent_pre_confirmation
    FROM peer_messages msg
        LEFT JOIN PRODUCTION.fact_transaction__tx_hash ft
            ON msg.tx_hash=ft.tx_hash
            AND msg.msg_timestamp < ft.blk_timestamp
)
, peer_get_messages as (
    select
          msg.node_id
        , msg.peer_id
        , msg.msg_timestamp
        , floor(date_part(epoch_second, MSG_TIMESTAMP) / 60) as bin_num
        , ARRAY_SIZE(msg_data)                                  AS msg_hash_count
        , HLL_ACCUMULATE(hashes.value)                          AS hash_hll
    from KEYSTONE_OFFCHAIN.NETWORK_FEED msg,
        LATERAL FLATTEN(input => msg_data) hashes
    WHERE msg_timestamp BETWEEN '2024-01-22' AND '2024-01-29'
        AND msg_type = 'get_tx_66'
    GROUP BY 1,2,3,4,5
)
, session AS (
    SELECT *
    FROM production.FACT_PEER_SESSION
    WHERE START_TIME BETWEEN '2024-01-22' AND '2024-01-29'
)
, session_bin AS (
    SELECT
          s.*
        , bins.bin_num
    FROM session s
        inner join bins
            on bins.bin_num between  floor(date_part(epoch_second, s.START_TIME) / 60) and floor(date_part(epoch_second, s.END_TIME) / 60)

)
, session_broadcast AS (
    SELECT 
          s.PEER_ID
        , s.NODE_ID
        , s.START_TIME
        , s.session_duration_calculated
        , count(conf.peer_id) as total_hash_count
        , div0(total_hash_count, (s.session_duration_calculated / 1e9 / 60)) as hash_per_minute
        , count(distinct conf.tx_hash) as confirmed_distinct_tx_count
        , div0(confirmed_distinct_tx_count, (s.session_duration_calculated / 1e9 / 60)) as confirmed_distinct_tx_per_minute
    from session_bin s
    left join confirmed conf
        on s.bin_num = conf.bin_num
            and conf.MSG_TIMESTAMP between date_trunc('second', s.start_time) and date_trunc('second', s.end_time)
            and s.PEER_ID = conf.PEER_ID
            and s.NODE_ID = conf.NODE_ID
    GROUP BY 1,2,3,4
)
, session_get AS (
    SELECT 
          s.PEER_ID
        , s.NODE_ID
        , s.START_TIME
        , s.session_duration_calculated
        , sum(get.msg_hash_count) as total_get_hash_count
        , div0(total_get_hash_count, (s.session_duration_calculated / 1e9 / 60)) as get_hash_per_minute
        , HLL_ESTIMATE(HLL_COMBINE(get.hash_hll)) as distinct_get_hash_count
        , div0(distinct_get_hash_count, (s.session_duration_calculated / 1e9 / 60)) as distinct_get_hash_per_minute
    from session_bin s
    left join peer_get_messages get
        on s.bin_num = get.bin_num
            and get.msg_timestamp between date_trunc('second', s.start_time) and date_trunc('second', s.end_time)
            and s.peer_id = get.peer_id
            and s.node_id = get.node_id
    GROUP BY 1,2,3,4
)
select
    s.PEER_ID,
    s.NODE_ID,
    s.START_TIME,
    s.END_TIME,
    s.SESSION_DURATION_CALCULATED,
    s.SESSION_DURATION_HOUR,
    s.MSG_CT,
    s.msg_per_minute,
    conf.total_hash_count,
    conf.hash_per_minute,
    conf.confirmed_distinct_tx_count,
    conf.confirmed_distinct_tx_per_minute,
    get.total_get_hash_count,
    get.get_hash_per_minute,
    get.distinct_get_hash_count,
    get.distinct_get_hash_per_minute
from session s
    left join session_broadcast conf
        on s.PEER_ID = conf.PEER_ID
            and s.NODE_ID = conf.NODE_ID
            and s.start_time = conf.start_time
    left join session_get get
        ON s.peer_id = get.peer_id
            and s.node_id = get.node_id
            and s.start_time = get.start_time