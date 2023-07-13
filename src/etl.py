import os
import glob
import mysql.connector
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Function that reads a json song file, proccesses it and inserts it into a database into two tables.

    Parameters
    ----------
    cur
        Cursor of the created database.
    filepath : str
        Filepath of the song json file.
    """

    # open song file
    df1 = pd.read_json(path_or_buf=filepath, lines=True)

    # Replace 'nan' values with None
    df1 = df1.where(pd.notnull(df1), 'NULL')

    # insert song record
    song_coln = ["song_id", "title", "artist_id", "year", "duration"]
    songs_dt = df1[song_coln].values[0].tolist()
    print(songs_dt[1])
    try:
        cur.execute(song_tbl_insert, songs_dt)
    except mysql.connector.IntegrityError:
        pass

    artist_coln = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artist_dt = df1[artist_coln].values[0].tolist()
    try:
        cur.execute(artist_tbl_insert, artist_dt)
    except mysql.connector.IntegrityError:
        # If duplicate entry exists, skip the insertion
        pass

def process_log_file(cur, filepath):
    """
    Function that reads a json log file, proccesses it and inserts it into a database into three tables.

    First function reads a json log file, filters it for the NextSong values in the page columns
    Timestamp column is converted and split into multiple columns, and inserted into a time table.
    User data is inserted into a user table in the database, and lastly songplays table is
    created from a combination of song and log data json files based on the join query.

    Parameters
    ----------
    cur
        Cursor of the created database.
    filepath : str
        Filepath of the log json file.
    """
    # open log file
    df1 = pd.read_json(path_or_buf=filepath, lines=True)

    # filter by NextSong action
    song_filter = df1["page"] == "NextSong"
    df1 = df1[song_filter]

    # convert timestamp column to datetime
    df1['start_time'] = pd.to_datetime(df1['ts'], unit='ms')

    # insert time data records
    time_dt = ["hour", "day", "week", "month", "year", "weekday"]
    coln_lab = ["start_time", "hour", "day", "month", "year", "weekday"]
    for t in time_dt:
        if t == 'week':
            df1[t] = df1.start_time.dt.isocalendar().week
        else:
            df1[t] = getattr(df1.start_time.dt, t)
    time_df = df1[coln_lab]

    for i, row in time_df.iterrows():
        cur.execute(time_tbl_insert, list(row))

    # load user table
    user_coln = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df1 = df1[user_coln]
    user_df1 = user_df1.drop_duplicates(subset=['userId'])
    # insert user records
    for i, row in user_df1.iterrows():
        cur.execute(user_tbl_insert, tuple(row))

    # insert songplay records
    for index, row in df1.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songply_tbl_insert = "INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        songply_data = (
        row['start_time'], row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'],
        row['userAgent'])
        cur.execute(songply_tbl_insert, songply_data)



def process_data(cur, conn, filepath, func):
    """
    Function gets all files matching a json extention from a directory,
    iterates over each file and processes it by a given function.

    Parameters
    ----------
    cur
        Cursor of the created database.
    conn
        Connection of the created database.
    filepath : str
        Filepath of the log json file.
    func
        function call in the argument
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    numbers_files = len(all_files)
    print('{} files found in {}'.format(numbers_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, numbers_files))



def main():
    """
    Main function tha establishes a connection to the database and creates a cursor.
    Proccesses the data for each log file and song file, and inserts it accordingly
    to the database tables.
    """
    conn = mysql.connector.connect(host="127.0.0.1", database="kikkify", user="root", passwd="zzzxxxZX1")
    cur = conn.cursor()
    process_data(cur, conn, filepath= 'C:/Users/Hp/Downloads/data-modeling-with-postgres-master/data-modeling-with-postgres-master/data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='C:/Users/Hp/Downloads/data-modeling-with-postgres-master/data-modeling-with-postgres-master/data/log_data', func=process_log_file)
    conn.close()


if __name__ == "__main__":
    main()