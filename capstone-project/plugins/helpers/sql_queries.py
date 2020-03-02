class SQLQueries:
    date_table_insert = ("""
        SELECT DISTINCT to_date(date,'YYYY-MM') AS date,
	                    EXTRACT(YEAR from to_date(date,'YYYY-MM')) AS year,
	                    EXTRACT(MONTH from to_date(date,'YYYY-MM')) AS month                              
        FROM staging_outcomes
        UNION
        SELECT DISTINCT to_date(month,'YYYY-MM') AS date,
	                    EXTRACT(YEAR from to_date(month,'YYYY-MM')) AS year,
	                    EXTRACT(MONTH from to_date(month,'YYYY-MM')) AS month                              
        FROM staging_crimes 

        ORDER BY date desc
    """)

    neighborhoods_table_insert = (""" 
        SELECT  id, name, population,
                address,e_messaging,facebook,
                fax,rss,telephone,twitter,
                website,youtube,
                CAST(centre_latitude AS FLOAT) centre_latitude ,
                CAST(centre_longitude AS FLOAT) centre_longitude
        FROM staging_neighborhoods
    """)

    crimes_table_insert = ("""
        SELECT c.id id,
               c.persistent_id persistent_id,
               c.category category,
               c.context context,
               c.location_type location_type,
               to_date(c.month,'YYYY-MM')  date,
               c.latitude latitude,
               c.longitude longitude,
               c.street_name street_name,
               n.id neighborhood_id,
               n.name neighborhood_name
        FROM staging_crimes c 
            INNER JOIN staging_neighborhoods n
            ON  c.neighborhood_id = n.id
    """)
    
    outcomes_table_insert = ("""
        SELECT DISTINCT o.category_name outcome,
	                    o.date date,
                        c.id crime_id,
                        c.neighborhood_id neighborhood_id,
                        n.force_id force_id
        FROM staging_crimes c 
            LEFT JOIN staging_outcomes o 
            ON o.persistent_id = c.Persistent_id
            JOIN staging_neighborhoods n
            ON c.neighborhood_id = n.id;
    """)