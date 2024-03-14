{{
    config(
        materialized='incremental'
      , post_hook=['{{ export_boot_node_to_s3() }}']
    )
}}

WITH latest_session AS (
    SELECT
          peer_id
        , peer_region
        , peer_public_key || '@' || peer_ip || ':' || peer_port AS enode
        , ROW_NUMBER() OVER(PARTITION BY peer_id ORDER BY start_time DESC) AS row_num
    FROM {{ ref('fact_peer_session') }}
    QUALIFY row_num = 1
)
, hash_ct AS (
    SELECT
          latest_session.peer_region
        , latest_session.enode
        , SUM(hourly.total_hash_count) AS hash_ct
    FROM {{ ref('peer_session_performance_hourly') }} hourly
        LEFT JOIN latest_session
            ON hourly.peer_id = latest_session.peer_id
    WHERE latest_session.peer_region IS NOT NULL
    GROUP BY 1,2
    HAVING hash_ct > 0
)
SELECT
      peer_region
    , enode
    , hash_ct
    , '{{run_started_at}}'::timestamp_ntz AS updated_at
FROM hash_ct
QUALIFY ROW_NUMBER() OVER(PARTITION BY peer_region ORDER BY hash_ct) <= 100
ORDER BY peer_region, hash_ct DESC
