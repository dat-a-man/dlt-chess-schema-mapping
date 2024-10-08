/* Table: players_online_status */
{{
    config(
        materialized='table'
    )
}}

-- depends_on: {{ ref('dlt_active_load_ids') }}

SELECT
/* select which columns will be available for table 'players_online_status' */
    username,
    online_status,
    last_login_date,
    check_time,
    _dlt_load_id,
    _dlt_id,
FROM {{ source('raw_data', 'players_online_status') }}

/* we only load table items of the currently active load ids into the staging table */
WHERE _dlt_load_id IN ( SELECT load_id FROM  {{ ref('dlt_active_load_ids') }} )