class CreateTableQueries:
    senior_officers_table_create = ("""
        CREATE TABLE IF NOT EXISTS senior_officers(
            officer_id  BIGINT IDENTITY PRIMARY KEY,     
            force_id    VARCHAR,
            name        VARCHAR,
            rank        VARCHAR,
            email       VARCHAR,
            telephone   VARCHAR,
            twitter     VARCHAR,
            website     VARCHAR
        )
    """) 
    
    locations_table_create = ("""
        CREATE TABLE IF NOT EXISTS neighborhood_locations(
            neighborhood_id     VARCHAR,
            address             VARCHAR,
            latitude            FLOAT,
            longitude           FLOAT,
            location_name       VARCHAR,
            postcode            VARCHAR,
            type                VARCHAR
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

    neighborhood_boundaries_table_create = ("""
        CREATE TABLE IF NOT EXISTS neighborhood_boundaries(
            neighborhood_id     VARCHAR,
            latitude            FLOAT,
            longitude           FLOAT
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
            street_name         VARCHAR,
            neighborhood_id     VARCHAR     NOT NULL,
            neighborhood_name   VARCHAR     NOT NULL
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
            crime_id                VARCHAR,
            neighborhood_id         VARCHAR,      
            force_id                VARCHAR
        ) 
    """)