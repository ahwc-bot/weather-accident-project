{{ config(
    materialized = 'table'
) }}

-- This model is a denormalized flat table for Tableau Public export.
-- Derived from int_enriched_incidents. Not intended for star schema joins.

with base as (
    select
        -- Identifier
        event_id,

        -- Temporal
        occ_date_utc,
        occ_date_day,
        occ_year,
        occ_month,
        occ_dow       as day_of_week,
        occ_hour      as hour_of_day,
        is_weekend,
        season,
        case
            when occ_hour between 0 and 5 then 'Night'
            when occ_hour between 6 and 11 then 'Morning'
            when occ_hour between 12 and 17 then 'Afternoon'
            else 'Evening'
        end as daypart,

        -- Location
        division,
        hood_158      as tps_hood_code,
        neighbourhood_158,
        lat,
        lon,
        invalid_coord_flag,

        -- Collision severity & outcomes
        collision_severity,
        collision_severity_rank,
        fatalities,
        injury_collisions,
        ftr_collisions,
        pd_collisions,

        -- Parties involved
        automobile,
        motorcycle,
        passenger,
        bicycle,
        pedestrian,

        -- Weather enrichments
        temperature,
        precipitation,
        snowfall,
        weathercode,
        weather_condition,
        windspeed,
        cloudcover,
        humidity,

        -- Audit
        inserted_at

    from {{ ref('int_enriched_incidents') }}
)

select *
from base