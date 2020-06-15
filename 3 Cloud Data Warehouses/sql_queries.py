import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

# The following drop table statements are run each time new tables
# are created

staging_events_table_drop = "drop table if exists events_stg;"
staging_songs_table_drop = "drop table if exists songs_stg;"
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists times;"

# CREATE TABLES

# The staging tables are created to load data directly from flat files
# into Redshift

# Subsequently, fact and dim tables are loaded from staging tables

staging_events_table_create= ("""
create table events_stg
(
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length float(5),
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration numeric(20,1),
    sessionId int,
    song varchar,
    status int,
    ts bigint,
    userAgent varchar,
    userId varchar
)
""")

staging_songs_table_create = ("""
create table songs_stg
(
    artist_id varchar,
    artist_latitude float(5),
    artist_location varchar,
    artist_longitude float(5),
    artist_name varchar,
    duration float(5),
    num_songs int,
    song_id varchar,
    title varchar,
    year int
)
""")

# The songplay fact table is loaded from the events staging table
# to hold event data associated with song play

# The data is distributed among nodes by the start_time, similar
# to how the log data files are divided

# The primary key of start_time and session will disallow duplicates

# The data will be sorted by start_time for quicker queries

songplay_table_create = ("""
create table songplays (
    start_time timestamp not null distkey sortkey,
    user_id varchar not null,
    level varchar,
    song_id varchar not null,
    artist_id varchar not null,
    session_id int,
    location varchar,
    user_agent varchar,
    primary key (start_time, session_id)
)
""")

# The dim table users is loaded from the events staging table

# The primary key of user_id will disallow duplicates

# The data will be sorted by user_id for quicker queries

user_table_create = ("""
create table users (
    user_id varchar not null primary key sortkey,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
)
""")

# The dim table users is loaded from the songs staging table

# The primary key of song_id will disallow duplicates

# The data will be sorted by song_id for quicker queries

song_table_create = ("""
create table songs (
    song_id varchar not null primary key sortkey,
    title varchar,
    artist_id varchar not null,
    year int,
    duration float(5)
)
""")

# The dim table artists is loaded from the songs staging table

# The primary key of artist_id will disallow duplicates

# The data will be sorted by artist_id for quicker queries

artist_table_create = ("""
create table artists (
    artist_id varchar not null primary key sortkey,
    name varchar,
    location varchar,
    latitude float(5),
    longitude float(5)
)
""")

# The dim table times is loaded from the events staging table

# The data is distributed among nodes by the start_time, similar
# to how the log data files were divided

# The primary key of start_time will disallow duplicates

# The data will be sorted by start_time for quicker queries

time_table_create = ("""
create table times (
    start_time timestamp not null primary key distkey sortkey,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
)
""")

# STAGING TABLES

# The staging tables are loaded from the log and song data flat files
# using the copy command so they can be ingested at scale

staging_events_copy = ("""
copy
    events_stg
from
    {}
credentials
    'aws_iam_role={}'
region 'us-west-2'
format as json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy
    songs_stg
from
    {}
credentials
    'aws_iam_role={}'
region 'us-west-2'
json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

# The fact table songplays is loaded from the events and songs staging
# tables by only loading the events associated with song play

# The song_id and artist_id columns are retrieved from the songs staging
# table by doing an outer join from events staging -> songs staging

# The distinct keyword is used to only load unique records, and only
# events associated with the NextSong action are loaded

songplay_table_insert = ("""
insert into
    songplays
select distinct
    timestamp 'epoch' + e.ts/1000 * interval '1 second',
    e.userId,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId,
    e.location,
    e.userAgent
from
    events_stg e
    inner join songs_stg s on
        e.song = s.title and
        e.artist = s.artist_name
where
    e.page='NextSong'
""")

# The dim table users is loaded from the events staging table
# by only loading the events associated with song play

# The distinct keyword is used to only load unique records, and only
# events associated with the NextSong action are loaded

user_table_insert = ("""
insert into
    users
select distinct
    userId,
    firstName,
    lastName,
    gender,
    level   
from
    events_stg
where
    page='NextSong'
""")

# The dim tables songs and artists are loaded from the songs staging table

# The distinct keyword is used to only load unique records

song_table_insert = ("""
insert into
    songs
select distinct
    song_id,
    title,
    artist_id,
    year,
    duration
from
    songs_stg
""")

artist_table_insert = ("""
insert into
    artists
select distinct
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
from
    songs_stg
""")

# The dim table times is loaded from the events staging table
# by only loading the events associated with song play

# The ts column is transformed into a timestamp and
# date parts are extracted for easy querying later

# The distinct keyword is used to only load unique records, and only
# events associated with the NextSong action are loaded

time_table_insert = ("""
insert into
    times
select distinct
    ts,
    extract(hour from ts),
    extract(day from ts),
    extract(week from ts),
    extract(month from ts),
    extract(year from ts),
    extract(dow from ts)
from
    (
        select
            timestamp 'epoch' + ts/1000 * interval '1 second' as ts
        from
            events_stg
        where
            page='NextSong'
    )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
