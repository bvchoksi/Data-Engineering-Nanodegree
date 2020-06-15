### Cloud Data Warehouse with S3 + Redshift Project

## Purpose

- A music streaming startup Sparkify wants to find insights into what songs their users are listening to. Their user activity data and song metadata resides in S3 in a series of JSON data files.

- The purpose of the project is to design a cloud data warehouse to hold the user activity and song metadata, and build an ETL pipeline that loads data from S3, stages it in Redshift, and transforms it into fact and dimension tables to enable analysis.

## Details

- This project entailed creating fact and dimension tables, and then designing an ETL process to load log and song data files into the fact and dimension tables.

- The fact and dimension tables reside in a Redshift cluster. The log and song data files are housed in S3.

- The tables are created using the python script files `sql_queries.py` and `create_tables.py`. The file `sql_queries.py` contains the DDL, while `create_tables.py` executes the DDL.

- The data is extracted, transformed and loaded using the python script file `etl.py`.

- The staging tables were loaded from S3 into Redshift using the copy command so that data could be ingested at scale, rather than line by line.

- The fact table `songplays` was structured taking into account that Redshift uses a multi-node architecture. The user activity data was loaded by distributing it across nodes by date and time so that queries are very responsive when analysis is done by week, month or year.

- The dimension table `times` was structured similarly to the fact table `songplays`, so that joins between them do not have to cross nodes. The other dimension tables were distributed across nodes sorted by their primary keys to optimise query times.

- The python scripts `create_tables.py` and `etl.py` were executed in order to ensure they ran without errors. The data loaded in Redshift was verified using the Redshift Query Editor.

## Dimension Tables

- Users

Contains customer information such as name, gender and level (Paid or Free).

- Songs

Contains song information such as title, artist, year and song duration.

- Artists

Contains artists' names and locations.

- Time

The start time in the song plays table is loaded here with its constituent parts such as hour, day, week, month, etc. for easy querying later.

## Fact Table

- Songplays

Contains information related to the songs played by customers such as start time, user ID, song, artist, location, user agent, etc.
