# Project: Data Warehouse
## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpose of this project is to build an ETL pipeline that extracts the data from S3, stages the data in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights in what songs the users are listening to. The database and ETL pipeline will be tested by running queries given by the analytics team from Sparkify and compare the results with their expected results.

## Project Description
In this project, data modelling with Redshift and ETL pipeline using Python are made. Following are different tasks which are done in the project:
1. Define the dimensional tables for the analytics team to continue finding insights in what songs the users are listening.
2. ETL pipeline that extracts the data from S3, stages the data in Redshift, and transforms data into a set of dimensional tables

## Datasets
All the datasets are in the json file format. There are two types of datasets:
1. **Song Dataset**: stored in s3://udacity-dend/song_data
2. **Log Dataset**: stored in s3://udacity-dend/log_data

## Schema Design
Staging tables along with Star Schema are optimized for queries on song play analysis is created in sql_queries.py. This includes the following tables:

### Staging Tables
1. **staging_events** - for staging the data in **Log Dataset**
|Field|Type|Null|Key|
|-----|----|----|---|
|artist|VARCHAR|YES||
|auth|VARCHAR|YES||
|firstName|VARCHAR|YES||
|gender|CHAR(1)|YES||
|itemInSession|INTEGER|YES||
|lastName|VARCHAR|YES||
|length|FLOAT|YES||
|level|VARCHAR|YES||
|location|VARCHAR|YES||
|method|VARCHAR|YES||
|page|VARCHAR|YES||
|registration|VARCHAR|YES||
|sessionID|INTEGER|YES||
|song|VARCHAR|YES||
|status|INTEGER|YES||
|ts|TIMESTAMP|YES||
|userAgent|VARCHAR|YES||
|userid|INTEGER|YES||

2. **staging_songs** - for staging the data in **Song Dataset**
|Field|Type|Null|Key|
|-----|----|----|---|
|num_songs|INTEGER|YES||
|artist_id|VARCHAR|YES||
|artist_latitude|FLOAT|YES||
|artist_longitude|FLOAT|YES||
|artist_location|VARCHAR|YES||
|artist_name|VARCHAR|YES||
|song_id|VARCHAR|YES||
|title|VARCHAR|YES||
|duration|FLOAT|YES||
|year|INTEGER|YES||

### Fact Table
3. **songplays** - records in log data associated with song plays i.e. records with page NextSong. 

    |Field|Type|Null|Key|
    |-----|----|----|---|
    |songplay_id|IDENTITY|NO|PRIMARY|
    |start_time|TIMESTAMP|NO|DIST, SORT|
    |user_id|INTEGER|NO||
    |level|VARCHAR|YES||
    |song_id|VARCHAR|NO||
    |artist_id|VARCHAR|NO||
    |session_id|INTEGER|NO||
    |location|VARCHAR|YES||
    |user_agent|VARCHAR|YES||

### Dimension Tables
4. **users** - users in the app

    |Field|Type|Null|Key|
    |-----|----|----|---|
    |user_id|INTEGER|NO|SORT, PRIMARY|
    |first_name|VARCHAR|NO||
    last_name|VARCHAR|NO||
    |gender|CHAR(1)|NO||
    |level|VARCHAR|YES||

5. **songs** - songs in music database

    |Field|Type|Null|Key|
    |-----|----|----|---|
    |song_id|VARCHAR|NO|SORT, PRIMARY|
    |title|VARCHAR|NO||
    |artist_id|VARCHAR|NO|REFERENCE|
    |year|INTEGER|NO||
    |duration|FLOAT|YES||

6. **artists** - artists in music database

    |Field|Type|Null|Key|
    |-----|----|----|---|
    |artist_id|VARCHAR|NO|SORT, PRIMARY|
    |name|VARCHAR|NO||
    |location|VARCHAR|YES||
    |latitude|FLOAT|YES||
    |longitude|FLOAT|YES||

7. **time** - timestamps of records in **songplays** broken down into specific units

    |Field|Type|Null|Key|
    |-----|----|----|---|
    |start_time|TIMESTAMP|NO|SORT, PRIMARY|
    |hour|INTEGER|NO||
    |day|INTEGER|NO||
    |week|INTEGER|NO||
    |month|INTEGER|NO||
    |year|INTEGER|NO||
    |weekday|INTEGER|NO||

### ETL Pipeline
Following are the different steps in the ETL pipeline:
* **Pre-requisites**- Database should be created in the redshift cluster and `dwh.cfg` file should be filled according to the database and cluster created. Now create the tables in the database by running the `create_tables.py` script by executing the following:
        python create_tables.py
        
* **Staging for log-data** The log dataset stored in s3 is staged in the table `staging_events` by copying the data into the table by executing the following SQL copy command:
        COPY staging_events FROM s3://udacity-dend/log_data:
        CREDENTIALS 'aws_iam_role=<DWH_ROLE_ARN>'
        JSON s3://udacity-dend/log_json_path.json TIMEFORMAT AS 'epochmillisecs'
        REGION 'us-west-2'

* **Staging for songs-data** The song dataset stored in s3 is staged in the table `staging_songs` by copying the data into the table by executing the following SQL copy command:
        COPY staging_songs FROM s3://udacity-dend/song_data
        CREDENTIALS 'aws_iam_role=<DWH_ROLE_ARN>'
        json 'auto' REGION 'us-west-2';
        
* **ETL from the staging tables** Following operations are performed on the staging tables:
    1. The two staging tables are joined and data for `songplays` is extracted from the resultant join.
    2. The data for `users` table is extracted from `staging_events` table
    3. The data for `songs`, `artists` and `time` tables is extracted from `staging_songs` table.
    
## Execution Instructions
1. Create a redshift cluster in the AWS cloud and create database
2. Fill up the `dwh.cfg` according to the database and cluster created earlier.
3. Create tables by running following in the terminal:
        python create_tables.py
4. Perform ETL by rinning the following in the terminal:
        python etl.py
        
## Example Queries
### 1. Get top 5 mostly viewed artists
**Query**:

    SELECT a.name AS artist_name, COUNT(*) AS count
    FROM songplays sp JOIN artists a ON sp.artist_id = a.artist_id
    GROUP BY a.Name
    ORDER BY COUNT DESC LIMIT 10;
    
**Results**:

|artist_name|count|
|-----------|-----|
|Dwight Yoakam|37|
|Kid Cudi / Kanye West / Common|10|
|Kid Cudi|10|
|Ron Carter|9|
|Lonnie Gordon|9|

### 2. Get the song mostly viewed by the Female users
**Query**:

    SELECT s.title AS song, u.gender, COUNT(*) AS count 
    FROM songplays sp JOIN songs s ON sp.song_id = s.song_id 
        JOIN users u ON sp.user_id = u.user_id
    WHERE u.gender = 'F'
    GROUP BY s.title, u.Gender
    ORDER BY count DESC 
    LIMIT 1;

**Results**:

|song|gender|count|
|----|------|-----|
|You're The One|F|26|

## Project File Structure
1. **create_redshift_cluster.ipynb** - creates redshift cluster and database in AWS. Needs `credentials.csv` file  from AWS account.
2. **credentials** - required by the **create_redshift_cluster.ipynb** to create red shift cluster. Copy `credentials.csv` from AWS user account(When you create it) here. 
3. **create_tables.py**- drops and creates the tables.
4. **etl.py**- Stages the data from s3 into the staging tables and then performs ETL on the staging tables in order to store the data in the star schema.
5. **README.md**- provides description on the project.
6. **sql_queries.py**- contains all the sql queries, and is imported into the last three files above.
7. **dwh.cfg**- contains the properties requires for establishing connected to the database in the redshift. It needs to be filled according to the database created in the cluster.
