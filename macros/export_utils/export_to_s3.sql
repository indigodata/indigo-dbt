{% macro export_to_s3(stage, file_name, query) -%}
    {% set stage_name = '@warehouse_db.production.' ~ stage %}
    {% set copy_query %}
        COPY INTO {{ stage_name }}/{{ file_name }}
        FROM (
            {{ query }}
        )
        FILE_FORMAT = (
            TYPE = 'CSV'
            EMPTY_FIELD_AS_NULL = FALSE
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('')
        )
        SINGLE=TRUE
        OVERWRITE=TRUE
        HEADER=TRUE
        MAX_FILE_SIZE = 52428800;
    {% endset %}

    {% if execute %}
        {% if should_full_refresh() and 'full_refresh' not in stage.lower() %}
            {{ log('This is a full refresh. Please use the full-refresh stage and copy it manually. Skipping the export.', 'warn') }}
        {% elif target.schema.lower() != 'production' %}
            {{ log('Not in the production schema. Skipping the export.', 'info') }}
        {% else %}
            {{ log('Copying data into S3 bucket', 'info') }}
            {% do log(copy_query, info=True) %}
            {% do run_query(copy_query) %}
        {% endif %}
    {% endif %}
{%- endmacro %}

{% macro export_undersampled_peers_to_s3() -%}
    {% set query %}
        SELECT 
              peer_region
            , enode
            , hash_ct
        FROM production.undersampled_peers
        WHERE updated_at = '{{ run_started_at }}'::timestamp_ntz
    {% endset %}
    {{ export_to_s3('s3_export_stage', 'undersampled_peers/undersampled_peers.csv.gz', query)}}
{%- endmacro %}

{% macro export_dim_peer_performance_to_s3() -%}
    {% set query %}
        SELECT 
              peer_id
            , avg_confirmed_distinct_tx_per_minute
            , avg_propogation_rate_2w
            , max_propogation_rate_2w
            , avg_propogation_rate
            , max_propogation_rate
            , peer_public_key
            , peer_rlp_protocol_version
            , peer_client_type
            , peer_client_version
            , peer_os
            , peer_run_time_version
            , peer_capabilities
            , peer_ip
            , peer_port
            , peer_region
            , peer_country
            , peer_city
            , last_seen_at
        FROM production.dim_peer_performance
        where last_seen_at >= sysdate() - interval '1 month'
    {% endset %}
    {{ export_to_s3('s3_export_stage', 'host_map/dim_peer_performance.csv.gz', query)}}
{%- endmacro %}