{{ config(materialized='ephemeral') }}

WITH node_tracker_base AS (
    SELECT 
          *
    FROM {{ source('keystone_offchain', 'node_tracker') }}
)

SELECT
      node_id
    , host AS ip
    , port
    , country
    , client as client_type
    , type as run_time_version
    , os
    , last_seen
    , '{{run_started_at}}'::timestamp_ntz  AS run_timestamp
FROM node_tracker_base
