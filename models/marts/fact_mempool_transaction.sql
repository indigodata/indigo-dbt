{{
    config(
        materialized='incremental'
      , cluster_by=['tx_hash']
      , unique_key=['tx_hash']
    )
}}

WITH new_hashes AS (
    SELECT
        hashes.value::STRING AS tx_hash
        , MIN(msg_timestamp) AS first_seen_at
    FROM {{ source('keystone_offchain', 'network_feed') }},
        LATERAL FLATTEN(input => msg_data) hashes
    {% if is_incremental() %}
        WHERE msg_timestamp > (SELECT MAX(first_seen_at) FROM {{ this }})
    {% else %}
        WHERE msg_timestamp > '2024-02-01 00:00:00'
    {% endif %}
            AND msg_type IN ('new_hash', 'new_hash_66', 'new_hash_68')
    GROUP BY 1
)
SELECT
      nh.tx_hash
    , nh.first_seen_at
    , '{{run_started_at}}'::timestamp_ntz   AS updated_at
FROM new_hashes nh
    LEFT JOIN {{ this }} this
        ON nh.tx_hash = this.tx_hash
WHERE nh.first_seen_at < this.first_seen_at
