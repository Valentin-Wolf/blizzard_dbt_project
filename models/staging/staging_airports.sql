SELECT a.*, r.region
FROM {{source('staging_flights', 'airports')}} AS a
LEFT JOIN {{source('staging_flights', 'regions')}} AS r
ON a.country = r.country