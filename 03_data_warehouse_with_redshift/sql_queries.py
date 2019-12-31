import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS times;"


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    event_id            BIGINT          IDENTITY(0,1)
                                        NOT NULL,
    artist              VARCHAR(256),
    auth                VARCHAR(30)     NOT NULL,
    firstName           VARCHAR(64),
    gender              CHAR(1),
    itemInSession       INTEGER         NOT NULL,
    lastName            VARCHAR(64),
    length              DECIMAL(15,5),
    level               VARCHAR(4)      NOT NULL,
    location            VARCHAR(128),
    method              VARCHAR(6)      NOT NULL,
    page                VARCHAR(30)     NOT NULL,
    registration        BIGINT,
    sessionId           BIGINT          NOT NULL,
    song                VARCHAR(256),
    status              INTEGER         NOT NULL,
    ts                  BIGINT          NOT NULL,
    userAgent           VARCHAR(256),
    userId              BIGINT
)
DISTKEY(sessionId)
SORTKEY(song)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id           VARCHAR(18)     NOT NULL,
    artist_latitude     DECIMAL(15,5),
    artist_location     VARCHAR(128),
    artist_longitude    DECIMAL(15,5),
    artist_name         VARCHAR(256)    NOT NULL,
    duration            DECIMAL(15,5)   NOT NULL,
    num_songs           INTEGER         NOT NULL,
    song_id             VARCHAR(18)     NOT NULL,
    title               VARCHAR(256)    NOT NULL,
    year                INTEGER         NOT NULL
)
DISTKEY(song_id)
SORTKEY(title)
""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id             INTEGER        NOT NULL,
    first_name          VARCHAR(64)    NOT NULL,
    last_name           VARCHAR(64)    NOT NULL,
    gender              CHAR(1)        NOT NULL,
    level               VARCHAR(4)     NOT NULL,
    primary key(user_id)
)
DISTKEY(user_id)
SORTKEY(user_id)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id           VARCHAR(18)     NOT NULL,
    artist_name         VARCHAR(256)    NOT NULL,
    artist_location     VARCHAR(128),
    artist_latitude     DECIMAL(15,5),
    artist_longitude    DECIMAL(15,5),
    primary key(artist_id)
)
DISTSTYLE ALL
SORTKEY(artist_id)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id             VARCHAR(18)     NOT NULL,
    title               VARCHAR(256)    NOT NULL,
    artist_id           VARCHAR(18)     NOT NULL,
    year                INTEGER         NOT NULL,
    duration            DECIMAL(15,5)   NOT NULL,
    primary key(song_id),
    foreign key(artist_id) references artists(artist_id)
)
DISTSTYLE ALL
SORTKEY(song_id)
""")



songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id         BIGINT         IDENTITY(0,1)
                                       NOT NULL,
    start_time          TIMESTAMP      NOT NULL,
    user_id             INTEGER        NOT NULL,
    level               VARCHAR(4)     NOT NULL,
    song_id             VARCHAR(18)    NOT NULL,
    artist_id           VARCHAR(18)    NOT NULL,
    session_id          BIGINT         NOT NULL,
    location            VARCHAR(128),
    user_agent          VARCHAR(256),
    primary key(songplay_id),
    foreign key(song_id) references songs(song_id),
    foreign key(artist_id) references artists(artist_id),
    foreign key(start_time) references times(start_time)
)
DISTKEY(session_id)
SORTKEY(start_time)
""")

#In practice there should be lots of songplay records an therefore timestamp cardinality should be very high.  This should serve the purpose of most time window queries assuming redshift distributes the time stamps uniformly accross the slices via consistent hashing. 
time_table_create = ("""
CREATE TABLE IF NOT EXISTS times (
    start_time          TIMESTAMP      NOT NULL,
    hour                SMALLINT       NOT NULL,
    day                 SMALLINT       NOT NULL,
    week                SMALLINT       NOT NULL,
    month               SMALLINT       NOT NULL,
    year                SMALLINT       NOT NULL,
    weekday             SMALLINT       NOT NULL,
    primary key(start_time)
)
DISTKEY(start_time)
SORTKEY(start_time)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
iam_role {}
json {}
region 'us-west-2'
TRUNCATECOLUMNS
BLANKSASNULL
EMPTYASNULL
MAXERROR 0
ACCEPTINVCHARS
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs FROM {}
iam_role {}
json 'auto'
region 'us-west-2'
TRUNCATECOLUMNS
BLANKSASNULL
EMPTYASNULL
MAXERROR 0
ACCEPTINVCHARS
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as setime,
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
FROM staging_events se
JOIN staging_songs ss
    ON (se.song = ss.title)
    AND (se.artist = ss.artist_name)
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    se.userId,
    se.firstName,
    se.lastName,
    se.gender,
    se.level
FROM staging_events se
WHERE se.page = 'NextSong'
""")

song_table_insert = ("""
INSERT into songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    ss.song_id,
    ss.title,
    ss.artist_id,
    ss.year,
    ss.duration
FROM staging_songs ss
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
SELECT DISTINCT
    ss.artist_id,
    ss.artist_name,
    ss.artist_location,
    ss.artist_latitude,
    ss.artist_longitude
FROM staging_songs ss
""")

time_table_insert = ("""
INSERT INTO times (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as setime, 
    EXTRACT(HOUR FROM setime),
    EXTRACT(DAY FROM setime),
    EXTRACT(WEEK FROM setime),
    EXTRACT(MONTH FROM setime),
    EXTRACT(YEAR FROM setime),
    EXTRACT(DOW FROM setime)
FROM staging_events se
WHERE se.page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        user_table_create, artist_table_create,
                        song_table_create, time_table_create,
                        songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert,user_table_insert, song_table_insert,
                        artist_table_insert, time_table_insert]

