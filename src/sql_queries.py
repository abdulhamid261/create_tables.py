# DROP TABLES
songply_tbl_dp = "DROP TABLE IF EXISTS songplays"
user_tbl_dp = "DROP TABLE IF EXISTS users"
song_tbl_dp = "DROP TABLE IF EXISTS songs"
artist_tbl_dp = "DROP TABLE IF EXISTS artists"
time_tbl_dp = "DROP TABLE IF EXISTS time"

# CREATE TABLES
songply_tbl_create = """CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR(250),
    song_id VARCHAR(250),
    artist_id VARCHAR(250),
    session_id VARCHAR(250),
    location VARCHAR(250),
    user_agent VARCHAR(250),
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
);"""

songplays = '''CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT AUTO_INCREMENT PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INT,
    level VARCHAR(50),
    song_id VARCHAR(100),
    artist_id VARCHAR(100),
    session_id INT,
    location VARCHAR(100),
    user_agent VARCHAR(255)
);'''

# Create the users table
user_tbl_create  = """
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(10),
    level VARCHAR(10)
);
"""
song_tbl_create = """CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(250) PRIMARY KEY,
    title VARCHAR(250),
    artist_id VARCHAR(250) NOT NULL,
    year INT,
    duration FLOAT
);"""

artist_tbl_create = """CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(250) PRIMARY KEY,
    name VARCHAR(250),
    location VARCHAR(250),
    lattitude VARCHAR(250),
    longitude VARCHAR(250)
);"""

time_tbl_create = """CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);"""

# INSERT RECORDS
songply_tbl_insert = """INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""

user_tbl_insert = "INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE first_name = VALUES(first_name), last_name = VALUES(last_name), gender = VALUES(gender), level = VALUES(level)"

song_tbl_insert = """INSERT INTO songs (
    song_id, title, artist_id, year, duration
) VALUES (%s, %s, %s, %s, %s);"""

artist_tbl_insert = """INSERT INTO artists (
    artist_id, name, location, lattitude, longitude
) VALUES (%s, %s, %s, %s, %s);"""

time_tbl_insert = """
    INSERT INTO time (start_time, hour, day, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s);"""

# FIND SONGS
song_select = """SELECT s.song_id AS song_id, a.artist_id AS artist_id
                FROM songs s JOIN artists a ON s.artist_id = a.artist_id
                WHERE s.title = %s AND a.name = %s AND s.duration = %s;"""

# QUERY LISTS
create_tbl_qur = [user_tbl_create, song_tbl_create, artist_tbl_create, time_tbl_create, songply_tbl_create]
dp_tbl_qur = [user_tbl_dp, song_tbl_dp, artist_tbl_dp, time_tbl_dp, songply_tbl_dp]
