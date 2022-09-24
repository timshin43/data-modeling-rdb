import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    ''' This function processes JSON data related to songs and loads it into 2 tables songs and artists. 
    The function receives a single path as an argument, multiple filepaths cannot be passed.'''
    
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values
    song_data = song_data[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name', 'artist_location', 'artist_latitude','artist_longitude']].values
    artist_data = artist_data[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    ''' This function processes JSON data from log files and loads it into tables: songplays, users, time
    The function receives a single path to a log_file as an argument, multiple  filepaths cannot be passed.'''
    
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']
    
    #convert ts to datetime and break it down into day,week etc.
    df['ts_date'] = pd.to_datetime(df['ts'],unit='ms')
    df['hour'],df['day'],df['week_of_year'],df['month'],df['year'],df['weekday'] =[df['ts_date'].dt.hour,df['ts_date'].dt.day,df['ts_date'].dt.week,df['ts_date'].dt.month,df['ts_date'].dt.year,df['ts_date'].dt.weekday]
    time_df = df[['ts_date','hour','day','week_of_year','month','year','weekday']]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts_date,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    ''' The function itterates through all JSON files and calls data proccessing functions for each itteration'''
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    ''' The function operates as a controler: it connects to a database and calls functions to activate the ETL process '''
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()