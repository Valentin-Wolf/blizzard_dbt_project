    WITH flights_full_table AS (
        SELECT * 
        FROM {{source('staging_flights', 'flights')}}
        WHERE DATE_PART('month', flight_date) = 1 
    )
    SELECT * FROM flights_full_table