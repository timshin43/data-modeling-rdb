#### Database Description
The database sparkifydb is designed to keep track of what songs users play. The database is build as a star schema and consists of the following tables:
* songplays (Fact Table) - records associated with song plays, users and artists etc. 
* users (Dimension Table) - users in the app
* songs (Dimension Table) - songs in music database
* artists (Dimension Table) - artists in music database
* time (Dimension Table) - timestamps of records in songplays broken down into specific units
The database sparkifydb allows to query all neccesary data about users, songs, artists, duration etc. and build reports based on it

#### How to run Python scripts
In order to create the database and launch the ETL process you need to run 2 py. scripts in the follwing order:
1. create_tables.py
2. etl.py
To run a python (py.) script open a terminal (File->New->Terminal) and make sure you are in the right directory by using a command *ls*. If you are in the directory with the script that you want to run then type *Python3 create_tables.py* or *Python3 etl.py*

#### Files in the repo
1. *test.ipynb* displays the first few rows of each table to let you check your database.
2. *create_tables.py* drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
3. *etl.ipynb* reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. *etl.py* reads and processes files from song_data and log_data and loads them into your tables.
5. *sql_queries.py* contains all your sql queries, and is imported into the last three files above.
6. *README.md* provides discussion on your project.


