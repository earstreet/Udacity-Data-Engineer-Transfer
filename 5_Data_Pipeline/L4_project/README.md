# Project 5: Data Pipelines with Airflow

## Description

### Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

It's up to me to create a high grade data pipeline that is dynamic and built from reusable tasks, can be monitored, and allows easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

### Project Description

In this project, I build an ETL pipeline with Apache Airflow. To complete the project, I create DAGs to load data from S3, process the data into analytics tables and check the quality of the data tables. I deploy this Airflow process on a Redshift cluster using AWS.

## Getting Started

The project contains the following files:

[dags/udac_example_dag.py](./dags/udac_example_dag.py): Script to run the ETL process with the following steps:

1. creating empty tables
2. staging events and songs to Redshift
3. loading played songs data in a fact table
4. loading dimension tables for users, songs, time and artists
5. running some quality checks 

[plugins/helpers/sql_queries.py](./plugins/helpers/sql_queries.py): Collection of SQL queries to insert data into the fact and dimension tables.
[plugins/operations/data_quality.py](./plugins/operations/data_quality.py): Operator to test the data quality of the tables.
[plugins/operations/load_dimension.py](./plugins/operations/load_dimension.py): Operator to insert the data from the staging tables to the dimension tables.
[plugins/operations/load_fact.py](./plugins/operations/load_fact.py): Operator to insert the data from the staging tables to the fact table.
[plugins/operations/stage_redshift.py](./plugins/operations/stage_redshift.py): Operator to connect to S3 in order to insert the data from S3 into the staging tables on the Redshift cluster.
[set_airflow.py](./set_airflow.py): Script to automate the configuration of the environmental settings in Airflow. 

### Prerequisites

The script uses Airflow, a platform to programmatically author, schedule and monitor workflows. In order to run it on your local machine please take a look to the official installation guide in the [Apache Airflow Documentation](https://airflow.apache.org/docs/stable/installation.html).

You also need access to the AWS services:

- IAM
- S3
- Redshift

### Starting

Before you start the Airflow UI from the console you have to write your own configuration data into `set_airflow.py`. After that you can easily copy the settings directly into Airflow configuration settings by:

```bash
python set_airflow.py
```

Start the Airflow UI with:

```bash
/opt/airflow/start.sh
```

In Airflow you will see the DAG which can be started by turning it on.