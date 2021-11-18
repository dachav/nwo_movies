-- Creation of day_dim table
CREATE TABLE IF NOT EXISTS day_dim (
  day_key SERIAL PRIMARY KEY,
  capture_date DATE,
  month_code SMALLINT,
  quarter_code SMALLINT,
  year INTEGER
);

-- Creation of movie_dim table
CREATE TABLE IF NOT EXISTS movie_dim (
  movie_key SERIAL PRIMARY KEY,
  imdb_id VARCHAR(30) NOT NULL,
  title VARCHAR(200),
  release_year INTEGER,
  runtime_minutes INTEGER,
  mpaa_rating VARCHAR(30),
  genre1 VARCHAR(50),
  genre2 VARCHAR(50),
  genre3 VARCHAR(50),
--  top50_genre_list VARCHAR(1000),
  summary VARCHAR(1000),
  actor1 VARCHAR(100),
  actor2 VARCHAR(100),
  actor3 VARCHAR(100),
  actor4 VARCHAR(100),
  director1 VARCHAR(100),
  director2 VARCHAR(100),
  director3 VARCHAR(100),
  director4 VARCHAR(100),
  director5 VARCHAR(100),
  director6 VARCHAR(100),
  director7 VARCHAR(100),
  director8 VARCHAR(100),
  director9 VARCHAR(100),
  director10 VARCHAR(100),
  director11 VARCHAR(100),
  director12 VARCHAR(100)
);

-- Creation of performance_fact table
CREATE TABLE IF NOT EXISTS movie_performance_fact (
  day_key INTEGER REFERENCES day_dim,
  movie_key INTEGER REFERENCES movie_dim,
  gross_earnings BIGINT,
  imdb_rating DECIMAL(2,1),
  metascore_rating INTEGER,
  num_votes INTEGER,
  timestamp TIMESTAMP,
  CONSTRAINT pk PRIMARY KEY (day_key,movie_key)
);

-- Creation of movie_performance_staging table
CREATE TABLE IF NOT EXISTS movie_performance_staging (
  staging_key SERIAL PRIMARY KEY,
  day_key INTEGER,
  capture_date DATE,
  month_code SMALLINT,
  quarter_code SMALLINT,
  year INTEGER,
  movie_key INTEGER,
  imdb_id VARCHAR(30) NOT NULL,
  title VARCHAR(200),
  release_year INTEGER,
  runtime_minutes INTEGER,
  mpaa_rating VARCHAR(30),
  genre1 VARCHAR(50),
  genre2 VARCHAR(50),
  genre3 VARCHAR(50),
--  top50_genre_list VARCHAR(1000),
  summary VARCHAR(1000),
  actor1 VARCHAR(100),
  actor2 VARCHAR(100),
  actor3 VARCHAR(100),
  actor4 VARCHAR(100),
  director1 VARCHAR(100),
  director2 VARCHAR(100),
  director3 VARCHAR(100),
  director4 VARCHAR(100),
  director5 VARCHAR(100),
  director6 VARCHAR(100),
  director7 VARCHAR(100),
  director8 VARCHAR(100),
  director9 VARCHAR(100),
  director10 VARCHAR(100),
  director11 VARCHAR(100),
  director12 VARCHAR(100),
  gross_earnings BIGINT,
  imdb_rating DECIMAL(2,1),
  metascore_rating INTEGER,
  num_votes INTEGER,
  timestamp TIMESTAMP
);