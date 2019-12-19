# Project: Data Modeling with Postgres
## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The aim of this project is to create a database schema and ETL pipeline for this analysis. The database and ETL pipeline will be tested by running queries given by the analytics team from Sparkify and compare the results with their expected results.

## Project Description
In this project, data modelling with Postgres and ETL pipeline using Python are made. Following are different task which are done in the project:
1. Define the fact and dimension tables for a star schema
2. ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Datasets
All the datasets are in the json file format. There are two types of datasets:
1. **Song Dataset**: stored in the `/data/song_data` location.
2. **Log Dataset**: stored in the `/data/log_data` location.

## Schema Design
Star Schema optimized for queries on song play analysis is created in `sql_queries.py`. This includes the following tables:
### Fact Table
1. **songplays** - records in log data associated with song plays i.e. records with page NextSong. 

    |Field|Type|Null|Key|
    |-----|----|----|---|
    |songplay_id|SERIAL|NO|PRIMARY|
    |start_time|TIMESTAMP|NO||
    |user_id|INT|NO|REFERENCE|
    |level|VARCHAR|YES||
    |song_id|VARCHAR|NO|REFERENCE|
    |artist_id|VARCHAR|NO|REFERENCE|
    |session_id|INT|NO||
    |location|VARCHAR|YES||
    |user_agent|VARCHAR|YES||

### Dimension Tables
2. **users** - users in the app

    |Field|Type|Null|Key|
    |-----|----|----|---|
    |user_id|INT|NO|PRIMARY|
    |first_name|VARCHAR|YES||
    last_name|VARCHAR|YES||
    |gender|CHAR(1)|NO||
    |level|VARCHAR|NO||

3. **songs** - songs in music database

    |Field|Type|Null|Key|
    |-----|----|----|---|
    |song_id|VARCHAR|NO|PRIMARY|
    |title|VARCHAR|NO||
    |artist_id|VARCHAR|NO|REFERENCE|
    |year|INT|NO| |
    |duration|NUMERIC|NO||

4. **artists** - artists in music database

    |Field|Type|Null|Key|
    |-----|----|----|---|
    |artist_id|VARCHAR|NO|PRIMARY|
    |name|VARCHAR|NO||
    |location|VARCHAR|YES||
    |latitude|NUMERIC|YES||
    |longitude|NUMERIC|YES||

5. **time** - timestamps of records in **songplays** broken down into specific units

    |Field|Type|Null|Key|
    |-----|----|----|---|
    |start_time|TIMESTAMP|NO|PRIMARY|
    |hour|INT|NO||
    |day|INT|NO||
    |week|INT|NO||
    |month|INT|NO||
    |year|INT|NO||
    |weekday|INT|NO||

## ETL Pipeline
Following are the different steps in the ETL pipeline:
* **Pre-requisites** - Database and table should be created. To create the database and tables run the `create_tables.py` file by running the following:

        python create_tables.py

* **ETL on song data** - In `etl.py` for each song files the following tasks are done:
    1. Fields `artist_id`, `artist_name`, `artist_location`,`artist_latitude` and `artist_longitude` are extracted from each song file and loaded into artists table in the database.
    2. Fields `song_id`, `title`, `artist_id`,`year` and `duration` are extracted from each songfile and loaded into songs table in the database.

* **ETL on log data** - In `etl.py` for each log files the following task are done:
    1. The data is filtered to only contain `NextSong` in the page field.
    2. Field `ts` is extracted from the data which is then converted into timestamp and from the timestamp the fields `hour`, `day`, `week`, `month`, `year`, `weekday` and then the data is loaded into `time` table.
    3. Fields `userId`, `firstName`, `lastName`, `gender` and `level` are extracted from each log data and loaded into the `users` table.
    4. For `songplays` table `song_id` and `artist_id` is searched from the joining of songs and artists table by matching `song`, `artist` and `duration` field with each record in log data. The rest of the fields in `songplays` dataset are also extracted from the data. 

## Execution Instructions
1. Create schema and table by running the following in the terminal:

        python create_tables.py
        
2. Perform ETL by rinning the following in the terminal:
    
        python etl.py

## Example Queries
### 1. Get total male and female users
**Query**:

    SELECT gender, COUNT(*) FROM users GROUP BY gender;
**Resuts**:

|gender|count|
|------|-----|
|F     | 55  |
|M     | 41  |

### 2. Get the only record from songplays where song_id is not null
**Qurey**:

    SELECT * FROM songplays WHERE song_id IS NOT NULL;

**Resutls**:

|songplay_id|start_time|user_id|level|song_id|artist_id|session_id|location|user_agent|
|-----------|----------|-------|-----|-------|---------|----------|--------|----------|
|3225|1970-01-01 00:25:42.837408|15|paid|SOZCTXZ12AB0182364|AR5KOSW1187FB35FF4|818|Chicago-Naperville-Elgin, IL-IN-WI|	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"|

## Project File Structure
1. **data** folder containing the data 
2. **create_tables.py** drops and creates the tables. Run this file to reset the tables before each time the ETL scripts are run.
3. **etl.ipynb** reads and processes a single file from song_data and log_data and loads the data into the tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. **etl.py** reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
5. **README.md** provides discussion on the project.
6. **sql_queries.py** contains all the sql queries, and is imported into the last three files above.
7. **test.ipynb** displays the first few rows of each table to check the database.

