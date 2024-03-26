{{
    config(
        materialized='incremental'
      , cluster_by=['tx_from']
    )
}}

SELECT *
FROM {{ ref('fact_hash_win__peer_id') }} fh
{% if is_incremental() %}
WHERE fh.confirmed_at > (SELECT MAX(confirmed_at)::timestamp FROM {{ this }})
{% endif %}
