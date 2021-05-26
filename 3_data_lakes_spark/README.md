## Project Description

### Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data
warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as
a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them
using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to
continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from
Sparkify and compare your results with their expected results.

### Project Description

This projects loads data from S3 into analytics tables using Spark. The data is filtered and transformed and loaded back
back into S3. The project creates an EMR cluster on AWS with master and slave nodes which will be used for processing
the spark steps.

### Project Datasets

Datasets which will be used for song and long data

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

### Project files

- **setup_env.py** : Creates EMR cluster for give configuration from emr.cfg files. Additional all necessary resources
  are created with executions of this scrit as IAM roles, buckets and the execution code is uploaded to S3
- **etl.py** : Loads data from S3 bucket for song dataset extract columns for song and artist table and write to parquet
  files which are saved in S3
- **emr.cfg** : Configuration files contains all necessary configuration key value pairs

### Execution

1. Setup aws credentials configuration in ~/.aws/config files and necessary configuration items in emr.cfg file
2. Execute setup.env file and for provisioning EMR cluster and copying etl.py to S3 and starting spark job execution
2. Results are available in the defined OUTPUT bucket in the configuration file
