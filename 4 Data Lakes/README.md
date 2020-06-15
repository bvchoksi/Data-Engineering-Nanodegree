### Data Lakes with S3 + Spark Project

## Purpose

- A music streaming startup Sparkify wants to find insights into what songs their users are listening to. Their user activity data and song metadata resides in S3 in a series of JSON data files.

- The purpose of the project is to design a data lake to hold the user activity and song metadata, and build an ETL pipeline that loads data from S3, analyses it with Spark, and loads it into fact and dimension parquet tables to enable analysis.

## Details

- This project entailed loading song and log data files in S3, and transforming the data into fact and dimension tables using Spark.

- The song and log data files in JSON format reside on S3. The fact and dimension tables also reside on S3 in parquet tables.

- The entire process of loading flat files, transforming them into a star schema, and then storing them in parquet tables is handled by the python script files `etl.py`.

- The fact table `songplays` was partitioned by year and artist.

- Of the dimension tables, `songs` was partitioned by year and artist, and `time` by year and month.

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

