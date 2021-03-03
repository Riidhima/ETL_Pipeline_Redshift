# Project 3: Cloud Data Warehouses

## Summary:

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Goals of this project are to:
* Build ETL pipeline extracting data from S3
* Load data from S3 into the staging tables in Redshift
* Transform data into a set of dimensional tables for the analytics team to continue finding insights in what songs their users are listening to
* Execute SQL statements 

### Datasets:
There are two datasets that we’ll work on. These reside in S3.
* **Song data -** This data is in JSON format and it contains metadata about a song and the artist of that song. The files in this dataset are partitioned by the first three letters of each song’s track ID. 
* **Log data -** This data comprises the user activity logs. The files in this dataset are partitioned by year and month.

**S3:** The public S3 bucket will be accessed by creating an IAM role. The data residing in S3 will be taken and transformed for faster processing and retrieval as per the STAR schema.

**Song data path** - s3://udacity-dend/song_data
**Log data path** - s3://udacity-dend/log_data

**Redshift:** It is a columnar storage database. In this project, we’ll be storing the data in Redshift using the ‘Auto’ distribution, where Redshift determines the optimal distribution based upon the size of the data. 

## How to run Python scripts:
1. Set up [can be done in 3 ways - AWS console, SDK or programmatic way]
* **AWS Redshift cluster:** with dc2.large and 4 nodes
* **IAM Role:** With ‘AmazonS3ReadOnlyAccess’ permission to access and read the data files from S3 public storage.
2. From the terminal run
* **python create_tables.py :** This to be run first in order to create the tables
* **python sql_queries.py :** This runs all CREATE, INSERT, COPY, DROP SQL statements
* **python etl.py :** This runs the ETL script and loads the data into the Fact and Dimension tables.


## Explanation of files:
* **sql_queries.py -** This file contains the queries to create the fact table and the dimension tables. Old tables are dropped prior to creating new tables. It also contains the queries to perform analysis on data.  
* **create_tables.py -** The python scripts responsible for creating the tables are in this file. It also contains the 
* **etl.py -** Implementing logic to  load data from S3 to staging tables and staging to Redshift
* **dwh.cfg -** Adding redshift database and IAM role info

