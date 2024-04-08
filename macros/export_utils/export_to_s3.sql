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
        {% elif target.schema.lower() == 'productions' %}
            {{ log('Not in the production schema. Skipping the export.', 'info') }}
        {% else %}
            {{ log('Copying data into S3 bucket', 'info') }}
            {% do log(copy_query, info=True) %}
            {% do run_query(copy_query) %}
        {% endif %}
    {% endif %}
{%- endmacro %}

{% macro export_undersampled_peers.sql_to_s3() -%}
    {% set query %}
        SELECT 
              peer_region
            , enode
            , hash_ct
        FROM production.undersampled_peers.sql
        WHERE updated_at = '{{ run_started_at }}'::timestamp_ntz
    {% endset %}
    {{ export_to_s3('s3_export_stage', 'undersampled_peers.sql/undersampled_peers.sql.csv.gz', query)}}
{%- endmacro %}