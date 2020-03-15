# Project: Data Pipelines with Airflow

## Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The purpose of this project is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Description
In this project the core concepts of Apache Airflow are explored. The custom operators are created 
to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step. The data is staged from S3 to redshift then the data warehouse is filled with the data.

## Datasets
All the datasets are in the json file format. There are two types of datasets:
1. **Song Dataset**: stored in s3://udacity-dend/song_data
2. **Log Dataset**: stored in s3://udacity-dend/log_data


## Configuring the DAG
In the DAG, the following parameters are added:

* The DAG does not have dependencies on past runs
* On failure, the task are retried 3 times
* Retries happen every 5 minutes
* Catchup is turned off
* Email are not sent on retry

## Building the operators
Four different operators are built that will stage the data, transform the data, and run checks on data quality.

Airflow's built-in functionalities as connections and hooks are utilized as much as possible and Airflow does all the heavy-lifting.

All of the operators and task instances run SQL statements against the Redshift database. 

### Stage Operator
The stage operator loads any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table. If the create_smt is specified then the stage table is also created.

### Fact and Dimension Operator
Facts and dimension load operators are created which load the tables according to the SQL statement passed in the operators. If create statement is specified then the operators also create the tables. Dimension loads are done with the truncate-insert pattern where the target table is emptied before the load. While fact loads are done with the append.

### Data Quality Operator
The data quality operator is also created which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.

## Pre-requisites
To run the dag the following pre-requisites must be met:
1. A redshift cluster should be running.
2. Create an AWS connection in airflow admin section with name `aws_credential` containing the ACCESS_KEY and SECRET_KEY.
3. Create a postgresql connection in airflow admin section with name `redshift` containing the redhsift database properties.

## Airflow DAG
The following dag in airflow is created as the end results:
<img src="images/dag.png">

Now Lets' look at each and every task in the DAG:
* **Stage_events**: Stages the logs data from S3 to the `staging_events` table. 
* **Stage_songs**: Stages the songs data from S3 to the `staging_songs` table. 
* **Load_songplays_fact_table**: Loads the data into `songplays` table.
* **Load_user_dim_table**: Loads the data into `user` table
* **Load_song_dim_table**: Loads the data into `songs` table
* **Load_artist_dim_table** : Loads the data into `artists` table
* **Load_time_dim_table**: Loads the data into `time` table
* **Run_data_quality_checks_songplays** : Runs the data quality checks on the `songplays` table.
* **Run_data_quality_checks_artists** : Runs the data quality checks on the `artists` table.
* **Run_data_quality_checks_users** : Runs the data quality checks on the `users` table.
* **Run_data_quality_checks_time** : Runs the data quality checks on the `time` table.
* **Run_data_quality_checks_songs** : Runs the data quality checks on the `songs` table.

## Project Structure
* **dags**: Folder containing the script which creates airflow dag.
* **plugins**: Folder containing the custom operators and helper functions.
* **README.md**: The file containing the summary of the project