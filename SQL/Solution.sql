CREATE TEMP Table capacity AS (
		SELECT 
			a.team_id,
			b.latitude AS base_latitude,
			b.longitude AS base_longitude,
			a.availability_from,
			a.availability_till,
			a.skill_level,
			CASE 	WHEN a.skill_level = 1 THEN 100
						WHEN a.skill_level = 2 THEN 22
						ELSE 21 END AS capacity
		FROM enpal.teams a
		LEFT JOIN enpal.bases b ON a.team_id=b.team_id
	);

CREATE TEMP Table all_routes AS (
	SELECT 
		b.customer_id,
		a.team_id, 
		b.latitude  AS destination_latitude,  
		b.longitude AS destination_longitude,
		a.base_latitude, 
		a.base_longitude, 
		SQRT(POW(111 * (base_latitude::float - destination_latitude::float), 2) + POW(111 * (base_longitude::float - destination_longitude::float) * COS(base_latitude::float), 2)) AS distance,
		b.start_date,
		a.availability_from, 
		a.availability_till, 
		CASE WHEN b.start_date >= a.availability_from AND b.start_date<a.availability_till THEN TRUE ELSE FALSE END availabile,
		b.number_of_panels,
		a.capacity,
		a.skill_level,
		CEIL(b.number_of_panels/a.capacity) AS required_day
	FROM capacity a
	CROSS JOIN enpal.customer_orders b
	ORDER BY 1,11,7,13
	);

CREATE TEMP Table availabile_routes AS (
	SELECT
		*
	FROM all_routes
	WHERE availabile=TRUE
	AND required_day=1
	ORDER BY 1,7,13
);


CREATE TEMP Table shortest_availabile_routes AS (
	SELECT
		a.customer_id,
		a.destination_latitude,
		a.destination_longitude,
		a.team_id,
		a.base_latitude,
		a.base_longitude,
		a.distance,
		a.start_date,
		a.availability_from,
		a.availability_till,
		a.availabile,
		a.number_of_panels,
		a.capacity,
		a.skill_level,
		a.required_day
	FROM availabile_routes a join
	     (
	     	SELECT
	     		customer_id,
	     		min(distance) AS distance
	     	FROM availabile_routes
	     	WHERE availabile=TRUE
				GROUP BY 1
	     ) b on a.customer_id = b.customer_id AND a.distance=b.distance
	ORDER BY 1,7,13 );

CREATE TEMP Table chosen_routes AS (
	SELECT
		a.customer_id,
		a.destination_latitude,
		a.destination_longitude,
		a.team_id,
		a.base_latitude,
		a.base_longitude,
		a.distance,
		a.start_date,
		a.availability_from,
		a.availability_till,
		a.availabile,
		a.number_of_panels,
		a.capacity,
		a.skill_level,
		a.required_day
	FROM (
	      SELECT
	      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) AS row_number,
	     	b.*
	      FROM ( SELECT * FROM shortest_availabile_routes ) b
	    ) a
	WHERE a.row_number = 1
	ORDER BY 1
	);

SELECT
	*
FROM chosen_routes
