    WITH flights_full_table AS (
        SELECT * 
        FROM {{source('staging_flights', 'flights_all_blizzard')}}
    )
    SELECT * FROM flights_full_table