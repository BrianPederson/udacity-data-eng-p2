## Project: Data Warehouse (2)
#### Data Engineering Nanodegree
##### Student: Brian Pederson
&nbsp;
#### Project Description
Using the fictitious startup Sparkify, build a dimensional star schema data model using AWS Redshift consisting of one fact and four dimensions. Write a basic ETL pipeline that transfers data from source json files stored in AWS S3 buckets using Python and SQL.

##### Data Sources (json files)
- song_data - s3://udacity-dend/song_data
- log_data - s3://udacity-dend/log_data

##### Data Targets (Data Warehouse Tables)
- songplays - fact table representing events associated with song plays
- users - dimension table representing users of the Sparkify service
- time - dimension table containing timestamps associated with songplay events
- songs - dimension table containing referenced songs
- artists - dimension table containing referenced artists

Note: all four dimension tables are relatively small so DISTSTYLE ALL is applied to those. The fact table and the two staging tables use DISTSTYLE EVEN.

##### Program files
- sql_queries.py - Script encapsulating all SQL statements.
- create_tables.py - Script to initialize environment by creating database and tables.
- etl.py - Script to implement simple ETL processes for DWH tables
- dwh.cfg - configuration file containing environment parameters (note AWS keys removed)
- README.md - this descriptive file

Note there are other files present in the project workspace used in development which are not technically part of the project submission.
- provisioning.ipynb - Jupyter notebook containing code snippets to startup and shutdown development Redshift cluster
- test.ipynb - Jupyter notebook containing various queries to examine contents of the staging and DWH tables
- etl.py - Python script used to develop and debug the SQL insert statements placed in sql_queries.py and used by etl.py
- log_json_path.json - configuration file (copied from location in s3 bucket) which overrides default mapping of log_data json files (note I made a copy of this in the local directory in order to study it)
- sql_queries_nofk.py - version of sql_queries.py containing queries without Foreign Keys applied

##### How to run the project
The process assumes :
- source data is located in an AWS s3 bucket with subdirectories song_data and log_data
- an IAM Role/ARN with approprirate read privs to the s3 data
- an AWS Redshift cluster has been spun up and is available
- the dwh.cfg file has been edited to apply all necessary configuration parms

Then run scripts:
1. Execute create_tables.py to create two staging and five DWH tables.
2. Execute etl.py to load data from s3 source files into two staging tables and from there into five DWH tables.
3. Optionally execute queries from test Jupyter notebook to observe contents of various tables.

##### Miscellaneous Notes
1. The test data provided is poorly configured. In particular the log/event data is not matched well against song/artist data. I noted that less than 5% of fact rows had matching songs/artists. In a normal DWH the dimension data should match closely with the fact data. This is because it's normal to use inner joins when joining fact to dimension tables. The missing dimension song/artist data causes sparse results for any inner join query which includes those dimensions. One workaround is to create dummy rows in the dimensions with a meaning of 'missing data'. Note that I have done that for this project so expect one extra row in those two dimensions.
2. I chose to only implement updates for the users dimension since it contains several columns that could change over time including level which is likely to change. For songs, artists, and time the insert statements ignore duplicate rows since the original data for these is as justifiable as subsequent data.
3. The design of the time dimension in this project is a little strange. It seems to have granularity down to the level of microseconds. I think in production the grain for this should be brought up to the second or even minute. As currently defined there is almost a one to one ratio between fact rows and time dimension rows. Normally dimensions should not grow as big as their accompanying fact tables.

