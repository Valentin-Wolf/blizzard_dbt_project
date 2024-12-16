WITH origin_data AS (
		SELECT f.*, a.name AS origin_name, a.city AS origin_city, a.country AS origin_country
		FROM {{ref('staging_flights')}} AS f
		LEFT JOIN {{ref('staging_airports')}} AS a
		ON f.origin = a.faa
	),
	dest_data AS (
		SELECT od.*, a.name AS dest_name, a.city AS dest_city, a.country AS dest_country
		FROM origin_data AS od
		LEFT JOIN airports AS a
		ON od.dest = a.faa
	)
	SELECT *
		FROM dest_data