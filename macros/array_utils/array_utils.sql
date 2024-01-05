{% macro func_find_duplicate_indices() -%}
CREATE OR REPLACE FUNCTION find_duplicate_indices(NODES_ORDERED ARRAY, PEERS_ORDERED ARRAY)
  RETURNS ARRAY
  LANGUAGE JAVASCRIPT
  AS $$
    var duplicates = [];
    var seen = {};

    for (var i = 0; i < NODES_ORDERED.length; i++) {
        var key = NODES_ORDERED[i] + '-' + PEERS_ORDERED[i];
        if (seen[key]) {
            duplicates.push(i);
        } else {
            seen[key] = true;
        }
    }

    return duplicates;
  $$;
{%- endmacro %}

{% macro func_remove_indices() -%}
CREATE OR REPLACE FUNCTION remove_indices(ARRAY_TO_PRUNE ARRAY, DUPLICATE_INDICES ARRAY)
  RETURNS ARRAY
  LANGUAGE JAVASCRIPT
  AS $$
    var result = [];

    for (var i = 0; i < ARRAY_TO_PRUNE.length; i++) {
        if (DUPLICATE_INDICES.indexOf(i) === -1) {
            result.push(ARRAY_TO_PRUNE[i]);
        }
    }

    return result;
  $$;
{%- endmacro %}