# Project: Data Warehouse

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.
Project Description

In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Requirements:
- an amazon iam user with the correct permissions to access s3 bucket
- a redshift cluster to act as a data warehouse
- python
- libraries : 
    > pandas 
    > psycopg2
    > configparser
    
## Directory map
> ROOT FOLDER
- create_tables.py
- dwh.cfg
- etl.py
- sql_queries.py


## Instructions to run:
- fill dwh.cfg with your redshift cluster instance / iam user (**do not upload your sensitive info on github**)
    > host
    > db_name
    > db_user
    > db_password
    > db_port
    > arn
- run create_tables.py
    > it drops/creates the necessary table 
- run etl.py
    > it inserts data extracted from staging tables staging_events and staging_songs
    
