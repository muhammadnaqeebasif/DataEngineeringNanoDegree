# Project: Data Lake
## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpose of this project is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

The database and ETL pipeline will be tested by running queries given by the analytics team from Sparkify and compare the results with their expected results.

## Project Description
In this project, the data will be loaded from S3, the data is processed using Spark and loaded back into S3. This Spark process will be deployed on a cluster using AWS.

## Datasets
All the datasets are in the json file format. There are two types of datasets:
1. **Song Dataset**: stored in s3://udacity-dend/song_data
2. **Log Dataset**: stored in s3://udacity-dend/log_data

## Schema Design
Using the song and log datasets, a star schema optimized for queries on song play analysis is created. This includes the following tables.

### Fact Table
1. **songplays** - records in log data associated with song plays i.e. records with page `NextSong`. 

    |Field|Type|Null|
    |-----|----|-----|
    |songplay_id|long|YES|
    |start_time|timestamp|YES|
    |user_id|string|YES|
    |level|string|YES|
    |song_id|string|YES|
    |artist_id|string|YES|
    |session_id|long|YES|
    |location|string|YES|
    |user_agent|string|YES|

### Dimension Tables
2. **users** - users in the app

    |Field|Type|Null|
    |-----|----|----|
    |user_id|string|YES|
    |first_name|string|YES|
    last_name|string|YES|
    |gender|string|YES|
    |level|string|YES|

3. **songs** - songs in music database

    |Field|Type|Null|
    |-----|----|----|
    |song_id|string|YES|
    |title|string|YES|
    |artist_id|string|YES|
    |year|long|YES|
    |duration|double|YES||

6. **artists** - artists in music database

    |Field|Type|Null|
    |-----|----|----|
    |artist_id|string|YES|
    |name|string|YES|
    |location|string|YES|
    |latitude|double|YES|
    |longitude|double|YES|

7. **time** - timestamps of records in **songplays** broken down into specific units

    |Field|Type|Null|
    |-----|----|----|
    |start_time|timestamp|YES|
    |hour|integer|YES|
    |day|integer|YES|
    |week|integer|YES|
    |month|integer|YES|
    |year|integer|YES|
    |weekday|integer|YES|

### ETL Pipeline
Following are the different steps in the ETL pipeline:
* **Pre-requisites** - `dl.cfg` should be filled according to the credentials.
        
* **ETL on song data** - In `etl.py` for each song files the following tasks are done:
    1. Fields `song_id`, `title`, `artist_id`,`year` and `duration` are extracted from each songfile and then the song_table is loaded into data lake(S3) in parquet format partitioned by `year` and `artist_id`.
    2. Fields `artist_id`, `artist_name`, `artist_location`,`artist_latitude` and `artist_longitude` are extracted from each song file and then the artist_table is loaded into data lake(S3) in parquet format.
    
* **ETL on log data** - In `etl.py` for each log files the following task are done:
    1. The data is filtered to only contain `NextSong` in the page field.
    2. Field `ts` is extracted from the data which is then converted into timestamp and from the timestamp the fields `hour`, `day`, `week`, `month`, `year`, `weekday` are extracted and then the data is loaded into data lake(S3) in parquet format partitioned by `year` and `month`.
    3. Fields `userId`, `firstName`, `lastName`, `gender` and `level` are extracted from each log data and then the data is loaded into data lake(S3) in parquet format.
    4. For `songplays` table `song_id` and `artist_id` is searched from the joining of songs by matching `song` field with each record in log data. The rest of the fields in `songplays` dataset are also extracted from the data and then the data is loaded into data lake(S3) in parquet format partitioned by `year` and `month`.
    
## Execution Instructions
1. Fill up the `dl.cfg` according to the credentials.
2. Perform ETL by rinning the following in the terminal:
        python etl.py
        
## Example Queries
### 1. Get top 5 mostly viewed artists
**Pyspark**:
```python
songplays_table\
    .join(artists_table,songplays_table.artist_id == artists_table.artist_id)\
    .groupBy('name')\
    .count()\
    .orderBy(desc('count'))\
    .limit(5)
```
**SQL Equivalent**:
```sql
SELECT a.name AS artist_name, COUNT(*) AS count
FROM songplays sp JOIN artists a ON sp.artist_id = a.artist_id
GROUP BY a.Name
ORDER BY COUNT DESC LIMIT 10;
```

**Results**:

|artist_name|count|
|-----------|-----|
|Dwight Yoakam|37|
|Carleen Anderson|17|
|Frozen Plasma|13|
|Working For A Nuclear Free City|13|
|Eli Young Band|13|

### 2. Get the song mostly viewed by the Female users
**Pyspark**
```python
songplays_table\
    .join(users_table,songplays_table.user_id == users_table.user_id)\
    .join(songs_table,songplays_table.song_id == songs_table.song_id)\
    .where("gender = 'F'")\
    .groupBy(col('title').alias('song'),'gender').count()\
    .orderBy(desc('count'))\
    .limit(1)
```
**SQL Equivalent**:
```sql
SELECT s.title AS song, u.gender, COUNT(*) AS count 
FROM songplays sp JOIN songs s ON sp.song_id = s.song_id 
     JOIN users u ON sp.user_id = u.user_id
WHERE u.gender = 'F'
GROUP BY s.title, u.Gender
ORDER BY count DESC 
LIMIT 1;
```

**Results**:

|song|gender|count|
|----|------|-----|
|Intro|F|25350|

## Project File Structure
1. **etl.py**- Performs the ETL on the data from S3 and stored the data back into data lake(S3).
2. **README.md**- provides description on the project.
3. **dl.cfg**- contains Amazon user credentials.