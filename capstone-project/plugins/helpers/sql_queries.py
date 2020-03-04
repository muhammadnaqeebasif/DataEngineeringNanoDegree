class SQLQueries:
    
    senior_officers_cols = ['force_id','name','rank','email', 
                            'telephone', 'twitter','website']
    
    neighborhood_locations_cols =['neighborhood_id','address','latitude',
                                  'longitude','location_name','postcode',
                                  'type']
    
    neighborhood_boundaries_cols = ['neighborhood_id', 'latitude', 'longitude']

    fact_outcomes_cols = ['outcome', 'date','crime_id',
                          'neighborhood_id','force_id']


    stage_table_insert = ("""
        SELECT * FROM {source}
        EXCEPT
        SELECT {cols} FROM {target}
    """)
    
    neighborhoods_table_insert = (""" 
        SELECT  id, name, population,
                address,e_messaging,facebook,
                fax,rss,telephone,twitter,
                website,youtube,
                CAST(centre_latitude AS FLOAT) centre_latitude ,
                CAST(centre_longitude AS FLOAT) centre_longitude
        FROM staging_neighborhoods
        EXCEPT
        SELECT *
        FROM dim_neighborhoods
    """)

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
        EXCEPT
        SELECT *
        FROM dim_date
    """)

    crimes_table_insert = ("""
        SELECT id,persistent_id,category,
               context,location_type,
               to_date(month,'YYYY-MM') date,
               latitude,longitude,
               street_name
        FROM staging_crimes
        EXCEPT
        SELECT *
        FROM dim_crimes
    """)
    
    senior_officers_insert_stmt = ("""
        INSERT INTO senior_officers (force_id,name,
                                     rank, email, 
                                     telephone, twitter,
                                     website)
    """)
    neighborhood_locations_insert_stmt = ("""
        INSERT INTO neighborhood_locations (neighborhood_id,
                                            address,
                                            latitude,
                                            longitude,
                                            location_name,
                                            postcode,
                                            type)
    """)

    neighborhood_boundaries_insert_stmt = ("""
        INSERT INTO neighborhood_boundaries (neighborhood_id, latitude, longitude)
    """)

    outcomes_insert_stmt = ("""
        INSERT INTO fact_outcomes (outcome, date,
                                   crime_id,
                                   neighborhood_id,
                                   force_id)
    """)

    outcomes_table_insert = ("""
        SELECT o.category_name outcome,
	                    to_date(o.date,'YYYY-MM') AS date,
                        c.id crime_id,
                        c.neighborhood_id neighborhood_id,
                        n.force_id force_id
        FROM staging_crimes c 
            LEFT JOIN staging_outcomes o 
            ON o.persistent_id = c.Persistent_id
            JOIN staging_neighborhoods n
            ON c.neighborhood_id = n.id
        EXCEPT
        SELECT {cols} FROM fact_outcomes
    """.format(cols=','.join(fact_outcomes_cols))
    )