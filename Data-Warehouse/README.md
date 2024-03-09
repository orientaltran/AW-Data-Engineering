# Project: Data Warehouse
## Project Desription
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift.
 To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Project Instructions
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

---

# Schema for Song Play Analysis

## CREATE TABLES
### `staging_events`
This table serves as an intermediate storage area for data ingested from user activity logs. It contains comprehensive details regarding the actions performed by users during their interactions with the music streaming service.
- **Columns**:  userId, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent,artist, auth, firstName, gender, itemInSession.

### `staging_songs`
This table functions as a transient storage repository for metadata related to songs and artists, acquired from the music dataset.

- **Columns**: song_id, artist_id, num_songs, artist_latitude, artist_name, title, duration, year, artist_longitude, artist_location.

## FINAL TABLES

### `songplays`
This table logs instances of song plays within the application and is employed for analyzing the usage patterns of the app. Each entry in the table corresponds to a user playing a specific song at a particular time, connecting to dimensions that provide details about the user, the content, and the location.

- **Columns**: song_id, artist_id, session_id, songplay_id, start_time, user_id, level, location, user_agent.
- **Primary Key**: songplay_id.

## DIMENSION TABLES
### `users`
Houses data pertaining to users utilizing the music streaming application.

- **Columns**: user_id, gender, level, first_name, last_name.
- **Primary Key**: user_id.

### `songs`
Comprises information regarding the songs accessible within the application.

- **Columns**: song_id, year, duration, title, artist_id.
- **Primary Key**: song_id.

### `artists`
Stores details concerning the artists associated with the songs.

- **Columns**: artist_id, longitude, name, location, latitude.
- **Primary Key**: artist_id.

### `time`
This table is designed to dissect timestamps of records into distinct units of time, facilitating time-based analysis
- **Columns**: start_time, hour, day, weekday, week, month, year.
- **Primary Key**: start_time.

---
### File

```shell
.
+-- create_table.py
+-- dwh.cfg
+-- etl.py
+-- sql_queries.py
```

### Edit config in ```dwh.cfg```

```config

[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=

[IAM_ROLE]
ARN=''

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[AWS]
REGION='us-west-2'

```

### Run command

The necessary files for submission encompass sql_queries.py, create_tables.py, and etl.py.

Upon the successful generation of dwh.cfg, the process proceeds to create tables and execute the ETL operation through the supplied Python scripts.

Run create tables
```python
python create_tables.py
```
Run ETL pipeline

```python
python etl.py
```