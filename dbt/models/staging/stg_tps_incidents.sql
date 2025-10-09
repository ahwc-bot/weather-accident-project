{{ config(
    materialized = 'view'
) }}

with source as (
    select *
    from {{ source('src', 'raw_incidents') }}
),

typed as (
    select
        -- Primary identifiers
        event_id,
        objectid,

        -- Extracted + typed fields from JSONB (attributes block)
        raw -> 'attributes' ->> 'EVENT_UNIQUE_ID' as event_unique_id,

        -- Dates and times (OCC_DATE in ms since epoch; hour in separate field)
        ((raw -> 'attributes' ->> 'OCC_DATE')::bigint / 1000)                              as occ_date_epoch,
        to_timestamp((raw -> 'attributes' ->> 'OCC_DATE')::bigint / 1000) AT TIME ZONE 'UTC' as occ_date,            -- raw anchor; time unreliable
        raw -> 'attributes' ->> 'OCC_MONTH'                                               as occ_month,           -- e.g., "March"
        raw -> 'attributes' ->> 'OCC_DOW'                                                 as occ_dow,             -- e.g., "Friday"
        (raw -> 'attributes' ->> 'OCC_YEAR')::int                                         as occ_year,
        (raw -> 'attributes' ->> 'OCC_HOUR')::int                                         as occ_hour,

        -- Categorical / text
        raw -> 'attributes' ->> 'DIVISION'                                                as division,
        raw -> 'attributes' ->> 'HOOD_158'                                                as hood_158,            -- e.g., "NSA"
        raw -> 'attributes' ->> 'NEIGHBOURHOOD_158'                                       as neighbourhood_158,

        -- Numeric counts + categorical booleans (YES/NO → boolean)
        coalesce((raw -> 'attributes' ->> 'FATALITIES')::int, 0)                           as fatalities,

        case raw -> 'attributes' ->> 'INJURY_COLLISIONS'
            when 'YES' then true
            when 'NO'  then false
            else null
        end as injury_collisions,

        case raw -> 'attributes' ->> 'FTR_COLLISIONS'
            when 'YES' then true
            when 'NO'  then false
            else null
        end as ftr_collisions,

        case raw -> 'attributes' ->> 'PD_COLLISIONS'
            when 'YES' then true
            when 'NO'  then false
            else null
        end as pd_collisions,

        -- Location from payload (attributes block: offset intersection coords)
        (raw -> 'attributes' ->> 'LONG_WGS84')::numeric(9,6)                               as long_wgs84,
        (raw -> 'attributes' ->> 'LAT_WGS84')::numeric(9,6)                                as lat_wgs84,

        -- Involvement flags (normalize YES/NO → boolean)
        case raw -> 'attributes' ->> 'AUTOMOBILE'
            when 'YES' then true
            when 'NO'  then false
            else null
        end as automobile,

        case raw -> 'attributes' ->> 'MOTORCYCLE'
            when 'YES' then true
            when 'NO'  then false
            else null
        end as motorcycle,

        case raw -> 'attributes' ->> 'PASSENGER'
            when 'YES' then true
            when 'NO'  then false
            else null
        end as passenger,

        case raw -> 'attributes' ->> 'BICYCLE'
            when 'YES' then true
            when 'NO'  then false
            else null
        end as bicycle,

        case raw -> 'attributes' ->> 'PEDESTRIAN'
            when 'YES' then true
            when 'NO'  then false
            else null
        end as pedestrian,

        -- Geometry block: tiny near-zero values in your sample (for completeness)
        (raw -> 'geometry' ->> 'x')::numeric                                              as geometry_x,
        (raw -> 'geometry' ->> 'y')::numeric                                              as geometry_y,

        -- Early extracted fields carried from raw_incidents table schema
        occ_date_utc,
        lat,
        lon,
        inserted_at

    from source
)

select *
from typed
