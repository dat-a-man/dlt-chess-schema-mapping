/* Table: players_online_status */
{{
    config(
        materialized='incremental'
    )
}}
SELECT
    t.username,
    t.online_status,
    t.last_login_date,
    t.check_time,
    t._dlt_load_id,
    t._dlt_id,
FROM  {{ ref('stg_players_online_status') }} as t