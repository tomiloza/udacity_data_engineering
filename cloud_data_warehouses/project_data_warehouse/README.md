## Project Description

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and
data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a
directory with JSON metadata on the songs in their app.

The task is to build a ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into
a set of dimensional tables for their analytics team to continue finding insights in what songs their users are
listening to.

### Database Schema

#### Staging tables

The data is first inserted from S3 buckets into intermediate staging tables:

- staging_songs - staging table about songs
- staging_events - table containing data about single user events

#### Fact table

songplays - this table contains single events from user interactions

As distribution key the **song_id** attribute is used because this will allow to evenly distribute the data between the
redshift nodes. For sortkey we ares using the **start_time** and **session_id** attributes because we suppose the
ordering of the data will be done mainly be these attributes.

#### Dimension tables

- users - users in the app
- songs - songs in music database
- artists - artists in music database
- time - timestamps of records in songplays broken down into specific units

### ETL procedure

Before processing and storing the data it is necessary to create the IAM roles and setup the redshift cluster.

For the setup and shutdown the environment foloing scripts are used:

- _setup_env.py_ - script used for creating necessary IAM roles and setup of redshift cluster
- _shutdown_env.py_ - script used for destroying of redshift cluster

After the setup of the environment following scripts has to be executed:

- create_tables.py - creates necessary staging and dimension/fact tables
- sql_queries.py - inserts data into the staging and dimension and fact tables from defined s3 buckets

### Configuration files

- dwh.cfg - configuration file used for defining AWS configuration items and redshift configuration

## OPTIONAL: Question for the reviewer

If you have any question about the starter code or your own implementation, please add it in the cell below.

For example, if you want to know why a piece of code is written the way it is, or its function, or alternative ways of
implementing the same functionality, or if you want to get feedback on a specific part of your code or get feedback on
things you tried but did not work.

Please keep your questions succinct and clear to help the reviewer answer them satisfactorily.

> By default I would suggest to use the ~/.aws/config file from the home directory to avoid saving or pushing AWS Keys to Github or Udacity Project space