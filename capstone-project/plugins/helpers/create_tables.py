class CreateTableQueries:
    """Class which contains the queries for creating the tables

    Attributes
    ----------
    staging_forces_table_create : str
        SQL statement for creating staging_forces table
    staging_senior_officers_table_create : str
        SQL statement for creating staging_senior_officers table
    staging_neighborhoods_table_create : str
        SQL statement for creating staging_neighborhoods table
    staging_neighborhood_locations_table_create : str
        SQL statement for creating staging_neighborhood_locations table
    staging_neighborhood_boundaries_table_create : str
        SQL statement for creating staging_neighborhood_boundaries table
    staging_crimes_table_create : str
        SQL statement for creating staging_crimes table
    staging_outcomes_table_create : str
        SQL statement for creating staging_outcomes table
    dim_forces_table_create : str
        SQL statement for creating dim_forces table
    senior_officers_table_create : str
        SQL statement for creating senior_officers table
    dim_neighborhoods_table_create : str
        SQL statement for creating dim_neighborhoods table
    neighborhood_locations_table_create : str
        SQL statement for creating neighborhood_locations table
    staging_neighborhood_boundaries_table_create : str
        SQL statement for creating staging_neighborhood_boundaries table
    dim_crimes_table_create : str
        SQL statement for creating dim_crimes table
    dim_date_table_create : str
        SQL statement for creating dim_date table
    fact_outcomes_table_create : str
        SQL statement for creating fact_outcomes table
    """
    staging_forces_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_forces (
            id          VARCHAR,
            name        VARCHAR,
            telephone   VARCHAR,
            url         VARCHAR,
            engagement_method_title VARCHAR,
            engagement_method_url VARCHAR
        )
    """)

    staging_senior_officers_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_senior_officers(  
            force_id    VARCHAR,
            name        VARCHAR,
            rank        VARCHAR,
            email       VARCHAR,
            telephone   VARCHAR,
            twitter     VARCHAR,
            website     VARCHAR
        )
    """)

    staging_neighborhoods_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_neighborhoods(
            id                  VARCHAR,
            name                VARCHAR,
            population          INTEGER,
            address             VARCHAR,
            e_messaging         VARCHAR,
            facebook            VARCHAR,
            fax                 VARCHAR,
            rss                 VARCHAR,
            telephone           VARCHAR,
            twitter             VARCHAR,
            website             VARCHAR,
            youtube             VARCHAR,
            centre_latitude    VARCHAR,
            centre_longitude    VARCHAR,
            force_id            VARCHAR
        )
    """)

    staging_neighborhood_locations_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_neighborhood_locations(
            neighborhood_id     VARCHAR,
            address             VARCHAR,
            latitude            FLOAT,
            longitude           FLOAT,
            location_name       VARCHAR,
            postcode            VARCHAR,
            type                VARCHAR
        )
    
    """) 

    staging_neighborhood_boundaries_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_neighborhood_boundaries(
            neighborhood_id     VARCHAR,
            latitude            FLOAT,
            longitude           FLOAT
        )
    """)

    staging_crimes_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_crimes(
            category            VARCHAR,
            location_type       VARCHAR,
            context             VARCHAR,
            persistent_id       VARCHAR,
            id                  BIGINT,
            location_subtype    VARCHAR,
            month               VARCHAR,
            neighborhood_id     VARCHAR,
            latitude            FLOAT,
            longitude           FLOAT,
            street_name         VARCHAR
        )
    """)

    staging_outcomes_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_outcomes(
            date                VARCHAR,
            person_id           VARCHAR,
            category_code       VARCHAR,
            category_name       VARCHAR,
            persistent_id       VARCHAR
        )
    """)

    dim_forces_table_create = ("""
        CREATE TABLE IF NOT EXISTS dim_forces (
            id          VARCHAR     PRIMARY KEY,
            name        VARCHAR,
            telephone   VARCHAR,
            url         VARCHAR,
            engagement_method_title VARCHAR,
            engagement_method_url VARCHAR
        )
    """)

    senior_officers_table_create = ("""
        CREATE TABLE IF NOT EXISTS senior_officers(
            officer_id  BIGINT IDENTITY PRIMARY KEY,     
            force_id    VARCHAR,
            name        VARCHAR,
            rank        VARCHAR,
            email       VARCHAR,
            telephone   VARCHAR,
            twitter     VARCHAR,
            website     VARCHAR,
            FOREIGN KEY(force_id) REFERENCES dim_forces(id)
        )
    """)

    dim_neighborhoods_table_create = ("""
        CREATE TABLE IF NOT EXISTS dim_neighborhoods(
            id                  VARCHAR     PRIMARY KEY,
            name                VARCHAR,
            population          INTEGER,
            address             VARCHAR,
            e_messaging         VARCHAR,
            facebook            VARCHAR,
            fax                 VARCHAR,
            rss                 VARCHAR,
            telephone           VARCHAR,
            twitter             VARCHAR,
            website             VARCHAR,
            youtube             VARCHAR,
            centre_latitude     FLOAT,
            centre_longitude    FLOAT
        )
    """)

    neighborhood_locations_table_create = ("""
        CREATE TABLE IF NOT EXISTS neighborhood_locations(
            location_id         BIGINT IDENTITY PRIMARY KEY, 
            neighborhood_id     VARCHAR,
            address             VARCHAR,
            latitude            FLOAT,
            longitude           FLOAT,
            location_name       VARCHAR,
            postcode            VARCHAR,
            type                VARCHAR,
            FOREIGN KEY(neighborhood_id) REFERENCES dim_neighborhoods(id)
        )
    
    """) 

    neighborhood_boundaries_table_create = ("""
        CREATE TABLE IF NOT EXISTS neighborhood_boundaries(
            boundary_id         BIGINT IDENTITY PRIMARY KEY, 
            neighborhood_id     VARCHAR,
            latitude            FLOAT,
            longitude           FLOAT,
            FOREIGN KEY(neighborhood_id) REFERENCES dim_neighborhoods(id)
        )
    """)

    dim_crimes_table_create = ("""
        CREATE TABLE IF NOT EXISTS dim_crimes(
            id                  BIGINT      PRIMARY KEY,
            persistent_id       VARCHAR,
            category            VARCHAR,
            context             VARCHAR,
            location_type       VARCHAR,
            date                DATE,
            latitude            FLOAT,
            longitude           FLOAT,
            street_name         VARCHAR
        )
    """)

    dim_date_table_create = ("""
        CREATE TABLE IF NOT EXISTS dim_date(
            date                DATE    PRIMARY KEY,
            year                INT,
            month               INT
        )
    """)

    fact_outcomes_table_create = ("""
        CREATE TABLE IF NOT EXISTS fact_outcomes(
            outcome_id              BIGINT      identity(0,1) PRIMARY KEY,
            outcome                 VARCHAR,
            date                    DATE,
            crime_id                BIGINT,
            neighborhood_id         VARCHAR,      
            force_id                VARCHAR,
            FOREIGN KEY(date) REFERENCES dim_date(date),
            FOREIGN KEY(crime_id) REFERENCES dim_crimes(id),
            FOREIGN KEY(neighborhood_id) REFERENCES dim_neighborhoods(id),
            FOREIGN KEY(force_id) REFERENCES dim_forces(id)
        ) 
    """)

