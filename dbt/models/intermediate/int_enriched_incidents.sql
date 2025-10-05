{{ config(
    materialized = 'table'
) }}

with stg as (
    select *
    from {{ ref('stg_tps_incidents') }}
),

-- Round coordinates for weather join and flag invalid coords
prep as (
    select
        *,
        round(lat::numeric, 2) as lat_r,
        round(lon::numeric, 2) as lon_r,

        -- Flag invalid coordinates (true = problem)
        case
            when lat is null or lon is null then true
            when lat < 40 or lat > 45 then true   -- rough Toronto bounds
            when lon < -80 or lon > -78 then true
            else false
        end as invalid_coord_flag,
		
		-- Collision severity hierarchy
        case
            when fatalities > 0 then 'Fatality'
            when injury_collisions then 'Injury'
            when ftr_collisions then 'Fail to Remain'
            when pd_collisions then 'Property Damage'
            else 'Unknown'
        end as collision_severity,

        -- Collision severity rank for easier sorting (1=worst, 5=least)
        case
            when fatalities > 0 then 1
            when injury_collisions then 2
            when ftr_collisions then 3
            when pd_collisions then 4
            else 5
        end as collision_severity_rank,

        -- Derived fields
        date_trunc('day', occ_date) as occ_date_day,
        case 
            when extract(isodow from occ_date) in (6,7) then true 
            else false 
        end as is_weekend,
        case
            when extract(month from occ_date) in (12,1,2) then 'Winter'
            when extract(month from occ_date) in (3,4,5) then 'Spring'
            when extract(month from occ_date) in (6,7,8) then 'Summer'
            else 'Fall'
        end as season

    from stg
),

-- Attach weather
with_weather as (
    select
        p.*,
        wc.temperature,
        wc.precipitation,
        wc.snowfall,
        wc.weathercode,
        wc.windspeed,
        wc.cloudcover,
        wc.humidity,
        -- Create weather condition buckets based on OM codes
        case
            when wc.weathercode = 0 then 'Clear'
            when wc.weathercode in (1,2,3) then 'Cloudy'
            when wc.weathercode in (45,48) then 'Fog'
            when wc.weathercode in (51,53,55,56,57) then 'Rain/Drizzle'
            when wc.weathercode in (61,63,65,66,67) then 'Rain'
            when wc.weathercode in (71,73,75,77,85,86) then 'Snow'
            when wc.weathercode in (80,81,82) then 'Rain showers'
            when wc.weathercode in (95,96,99) then 'Thunderstorm'
            else 'Other'
        end as weather_condition

    from prep p
    left join {{ source('src', 'weather_cache') }} wc
        on wc.lat = p.lat_r
       and wc.lon = p.lon_r
       and wc.hour_utc = p.occ_date_utc
)

select *
from with_weather
