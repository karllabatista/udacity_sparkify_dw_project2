import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY           = config.get('AWS','KEY')
SECRET        = config.get('AWS','SECRET')
LOG_DATA      = config.get('S3','LOG_DATA')
LOG_JSONPATH  = config.get('S3','LOG_JSONPATH')
SONG_DATA     = config.get('S3','SONG_DATA')
REGION        = config.get('S3','REGION')

# DROP TABLES

staging_events_table_drop = "DROP TABLE staging_events;"
staging_songs_table_drop = "DROP TABLE staging_songs;"
songplay_table_drop = "DROP TABLE songplay;"
user_table_drop = "DROP TABLE users;"
song_table_drop = "DROP TABLE song;"
artist_table_drop = "DROP TABLE artist;"
time_table_drop = "DROP TABLE time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist VARCHAR(500) NOT NULL,
        auth VARCHAR(200),
        first_name VARCHAR(500) NOT NULL,
        gender CHAR(1),
        itemInSession SMALLINT,
        last_name VARCHAR(500) NOT NULL,
        length DECIMAL(6),
        level CHAR(20),
        location VARCHAR(500),
        method CHAR(3),
        page VARCHAR(100) NOT NULL,
        registration  BIGINT,
        sessionId INTEGER,
        song VARCHAR(500) NOT NULL,
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR(500),
        userID INTEGER NOT NULL                                                                                       

    )
    DISTSTYLE ALL;
                               
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        song_id integer identity (0,1) PRIMARY KEY,
        title varchar(300) not null,
        artist_name varchar(300) not null,
        year integer ,
        duration integer 
    )
    DISTSTYLE ALL;
                                                          
""")


#songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay
    (
    
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    level CHAR(20),
    song_id INTEGER NOT NULL,
    artist_id INTEGER NOT NULL,
    session_id INTEGER NOT NULL,
    location VARCHAR(500),
    user_agent VARCHAR(100)                    
                         
    );
  
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users 
    (                 
        user_id INTEGER IDENTITY (0,1) PRIMARY KEY,
        first_name VARCHAR(500) NOT NULL,
        last_name  VARCHAR(500) NOT NULL,
        gender CHAR(1) ,
        level VARCHAR(20) 
    )
    DISTSTYLE ALL;               
    
                     
""")

song_table_create = ("""
    CREATE TABLE  IF NOT EXISTS song
    (                 
        song_id  INTEGER IDENTITY (0,1) PRIMARY KEY,
        title VARCHAR(500) NOT NULL,
        last_name  VARCHAR(500) NOT NULL,
        year integer NOT NULL,
        duration  DECIMAL(7,5) NOT NULL
    )
    DISTSTYLE ALL;               
    
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist
    (
        artist_id INTEGER PRIMARY KEY,
        name VARCHAR(500) NOT NULL,
        location VARCHAR(500),
        latitude FLOAT,
        longitude FLOAT
        
    )
    DISTSTYLE ALL;

""")
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS TIME
    (
        start_time TIMESTAMP NOT NULL,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        year INTEGER NOT NULL,
        weekday VARCHAR(100)
                                 
    )
    
    DISTSTYLE ALL; 
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM  {}
    CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
    JSON {}
    REGION {};

""").format(LOG_DATA,KEY,SECRET,LOG_JSONPATH,REGION)

staging_songs_copy = ("""
    COPY staging_songs
    FROM  {}
    CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
    FORMAT as JSON 'auto'
    REGION {};

""").format(SONG_DATA,KEY,SECRET,REGION)
            
# FINAL TABLES

songplay_table_insert = ("""

""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy]
#insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

insert_table_queries = []
