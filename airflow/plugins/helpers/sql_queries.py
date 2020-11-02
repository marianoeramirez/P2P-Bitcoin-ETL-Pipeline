class SqlQueries:
    create_table = ("""
    
CREATE TABLE IF NOT EXISTS public.staging_bisq (
	price decimal(16, 8),
	amount decimal(16, 8),
	volume decimal(16, 8),
	payment_method varchar(100),
	trade_date BIGINT,
	market varchar(100)
);

CREATE TABLE IF NOT EXISTS public.staging_paxful (
	id BIGINT,
	date BIGINT,
	amount decimal(16, 8),
	price decimal(16, 8),
	payment_method varchar(255),
	payment_method_group varchar(255),
	currency varchar(20),
	type varchar(50),
	advertiser_cc varchar(20),
	user_cc varchar(20),
	crypto_rate_usd decimal(16, 8),
	crypto_code varchar(100)
);

CREATE TABLE IF NOT EXISTS public.transaction (
	id varchar(32) NOT NULL,
	date timestamp NOT NULL,
	price decimal(16, 8),
	amount decimal(16, 8),
	payment_method varchar(255),
	currency_id int,
	crypto_id int,
    type varchar(10),
	CONSTRAINT transaction_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS public."time" (
	date timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (date)
);

CREATE TABLE IF NOT EXISTS public.provider (
	id int4 NOT NULL,
	name varchar(256),
	CONSTRAINT provider_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.currency (
	id int4 NOT NULL,
	name varchar(256),
	type varchar(255),
	CONSTRAINT currency_pkey PRIMARY KEY (id)
);


    """)
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)