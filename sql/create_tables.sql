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
  genres VARCHAR(1000),
  summary VARCHAR(1000),
  actors VARCHAR(2000),
  directors VARCHAR(4000)
);

-- Creation of performance_fact table
CREATE TABLE IF NOT EXISTS movie_performance_fact (
  day_key INTEGER REFERENCES day_dim,
  movie_key INTEGER REFERENCES movie_dim,
  imdb_rank VARCHAR(1000),
  gross_earnings BIGINT,
  imdb_rating DECIMAL(2,1),
  metascore_rating INTEGER,
  num_votes INTEGER,
  file_name VARCHAR(100),
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
  genres VARCHAR(1000),
  imdb_rank VARCHAR(1000),
  summary VARCHAR(1000),
  actors VARCHAR(2000),
  directors VARCHAR(4000),
  gross_earnings BIGINT,
  imdb_rating DECIMAL(2,1),
  metascore_rating INTEGER,
  num_votes INTEGER,
  file_name VARCHAR(100),
  timestamp TIMESTAMP
);