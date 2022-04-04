# Sparkify PostgreSQL ETL Project

---

This project practiced the following concepts:
- Data modeling with PostgreSQL
- Database star schemas
- ETL pipeline creation using Python

## Project Background

#### Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

#### Project Description

In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Data

- **Song Dataset**: All json files are nested in subdirectories under `/data/song_data`. A formatting sample is as follows:
```
num_songs:1, artist_id:"ARD7TVE1187B99BFB1", artist_latitude:null, artist_longitude:null, artist_location:"California - LA", artist_name:"Casual", song_id:"SOMZWCG12A8C13C480", title:"I Didn't Mean To", duration:218.93179, year:0
```
- **Log Dataset**: All json files are nested in subdirectories under `/data/log_data`. A formatting sample is as follows:
```
"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"
```

## Project Files

- **data** folder: Contains all json data files.
- **create_tables.py**: Drops old tables and creates new tables in database.
- **etl.ipynb**: Reads and processes a single file from both song_data and log_data. Loads data into your tables.
- **etl.py**: Reads and processes all files in the `/data/` directory and all subdirectories. Loads data into your tables.
- **sql_queries.py**: Contains all of the SQL queries used by *etl.py* file.
- **test.ipynb**: Checks the created database by displaying the first five rows in each table.
- **README.md**: Project information file.

## How to Use

#### Run the following in the project's directory terminal:
```
python create_tables.py
```
```
python etl.py
```

---

# Project Instructions

## Schema for Song Play Analysis
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

#### Fact Table

1. **songplays** - records in log data associated with song plays i.e. records with page `NextSong`
  * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables

1. **users** - users in the app
  * user_id, first_name, last_name, gender, level
2. **songs** - songs in music database
  * song_id, title, artist_id, year, duration
3. **artists** - artists in music database
  * artist_id, name, location, latitude, longitude
4. **time** - timestamps of records in songplays broken down into specific units
  * start_time, hour, day, week, month, year, weekday

## Project Template

To get started with the project, go to the workspace on the next page, where you'll find the project template files. You can work on your project and submit your work through this workspace. Alternatively, you can download the project template files from the Resources folder if you'd like to develop your project locally.

In addition to the data files, the project workspace includes six files:

1. `test.ipynb` displays the first few rows of each table to let you check your database.
2. `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
3. `etl.ipynb` reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. `etl.py` reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
5. `sql_queries.py` contains all your sql queries, and is imported into the last three files above.
6. `README.md` provides discussion on your project.

## Project Steps

**NOTE:** You will not be able to run test.ipynb, etl.ipynb, or etl.py until you have run create_tables.py at least once to create the sparkifydb database, which these other files connect to.

Below are steps you can follow to complete the project:

#### Create Tables

1. Write `CREATE` statements in `sql_queries.py` to create each table.
2. Write `DROP` statements in `sql_queries.py` to drop each table if it exists.
3. Run `create_tables.py` to create your database and tables.
4. Run `test.ipynb` to confirm the creation of your tables with the correct columns. Make sure to click "Restart kernel" to close the connection to the database after running this notebook.

#### Build ETL Processes

Follow instructions in the `etl.ipynb` notebook to develop ETL processes for each table. At the end of each table section, or at the end of the notebook, run `test.ipynb` to confirm that records were successfully inserted into each table. Remember to rerun `create_tables.py` to reset your tables before each time you run this notebook.

#### Build ETL Pipeline

Use what you've completed in `etl.ipynb` to complete `etl.py`, where you'll process the entire datasets. Remember to run `create_tables.py` before running `etl.py` to reset your tables. Run `test.ipynb` to confirm your records were successfully inserted into each table.

#### Run Sanity Tests

When you are satisfied with your work, run the cell under the **Sanity Tests** section in the `test.ipynb` notebook. The cells contain some basic tests that will evaluate your work and catch any silly mistakes. We test column data types, primary key constraints and not-null constraints as well look for on-conflict clauses wherever required. If any of the test cases catches a problem, you will see a warning message printed in Orange that looks like this:

```
[WARNING] The songplays table does not have a primary key!
```

You may want to make appropriate changes to your code to make these warning messages go away. The tests below are only meant to help you make your work foolproof. The submission will still be graded by a human grader against the project rubric.

#### Document Process
1. Do the following steps in your README.md file.
  * Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
  * How to run the Python scripts
  * An explanation of the files in the repository
  * State and justify your database schema design and ETL pipeline.
  * [Optional] Provide example queries and results for song play analysis.
2. Here's a [guide](https://www.markdownguide.org/basic-syntax/) on Markdown Syntax.
3. Provide DOCSTRING statement in each function implementation to describe what each function does.
