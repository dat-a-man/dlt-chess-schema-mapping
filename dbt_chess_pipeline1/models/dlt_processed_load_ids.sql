{{
    config(
        materialized='incremental'
    )
}}
-- depends_on: {{ ref('dim__dlt_loads') }}
-- depends_on: {{ ref('dim_players_profiles') }}
-- depends_on: {{ ref('dim_players_games') }}
-- depends_on: {{ ref('dim_players_profiles__streaming_platforms') }}
-- depends_on: {{ ref('dim_players_online_status') }}
/* we save all currently active load ids into the processed ids table */
select load_id, {{ current_timestamp() }} as inserted_at FROM {{ ref('dlt_active_load_ids') }}