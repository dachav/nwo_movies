import logging

import pandas as pd
from sqlalchemy import create_engine

import util

log = logging.getLogger(__name__)


def get_new_time_dim(conn_str):
    engine = create_engine(conn_str)
    new_time_dim_df = pd.read_sql_query("""
        SELECT DISTINCT 
            capture_date,
            month_code,
            quarter_code,
            year
        FROM
            movie_performance_staging
        EXCEPT
        SELECT DISTINCT
            capture_date,
            month_code,
            quarter_code,
            year
        FROM
            day_dim
    """, con=engine)

    return new_time_dim_df


def get_new_movie_dim(conn_str):
    engine = create_engine(conn_str)
    new_movie_dim_df = pd.read_sql_query("""
        SELECT DISTINCT 
            imdb_id,
            title,
            release_year,
            runtime_minutes,
            mpaa_rating,
            genre1,
            genre2,
            genre3,
            summary,
            actor1,
            actor2,
            actor3,
            actor4,
            director1,
            director2,
            director3,
            director4,
            director5,
            director6,
            director7,
            director8,
            director9,
            director10,
            director11,
            director12
        FROM
            movie_performance_staging
        EXCEPT
        SELECT DISTINCT
            imdb_id,
            title,
            release_year,
            runtime_minutes,
            mpaa_rating,
            genre1,
            genre2,
            genre3,
            summary,
            actor1,
            actor2,
            actor3,
            actor4,
            director1,
            director2,
            director3,
            director4,
            director5,
            director6,
            director7,
            director8,
            director9,
            director10,
            director11,
            director12
        FROM
            movie_dim
    """, con=engine)

    return new_movie_dim_df


def create_new_dim_values(conn_str):
    new_time_dim = get_new_time_dim(conn_str)
    new_movie_dim = get_new_movie_dim(conn_str)

    if new_time_dim.empty:
        log.warning("No data added to day_dim")
    else:
        util.ingest_df_into_sql(new_time_dim, conn_str, "day_dim", "append")
        log.info("Added data to day_dim")

    if new_movie_dim.empty:
        log.warning("No data added to movie_dim")
    else:
        util.ingest_df_into_sql(new_movie_dim, conn_str, "movie_dim", "append")
        log.info("Added data to movie_dim")


def populate_day_key_staging_table(conn_str):
    sql_statement = """
    UPDATE
        movie_performance_staging
    SET
        day_key = day_dim.day_key
    FROM
        day_dim
    WHERE
        movie_performance_staging.capture_date = day_dim.capture_date
    """

    util.run_crud_operation(conn_str, sql_statement)

    log.info("Populated day_key in staging")


def populate_movie_key_staging_table(conn_str):
    sql_statement = """
    UPDATE
        movie_performance_staging
    SET
        movie_key = movie_dim.movie_key
    FROM
        movie_dim
    WHERE
        movie_performance_staging.imdb_id = movie_dim.imdb_id
    """

    util.run_crud_operation(conn_str, sql_statement)

    log.info("Populated movie_key in staging")


def get_movie_key_for_delta(conn_str):
    engine = create_engine(conn_str)
    delta_df = pd.read_sql_query("""
                SELECT DISTINCT 
                    movie_key,
                    imdb_rank,
                    gross_earnings,
                    imdb_rating,
                    metascore_rating,
                    num_votes
                FROM
                    movie_performance_staging
                EXCEPT
                SELECT DISTINCT 
                    movie_key,
                    imdb_rank,
                    gross_earnings,
                    imdb_rating,
                    metascore_rating,
                    num_votes
                FROM
                    movie_performance_fact
            """, con=engine)
    movie_key_int_list = delta_df["movie_key"].astype(int).to_list()
    return [str(m) for m in movie_key_int_list]


def populate_movie_fact_table(conn_str):
    engine = create_engine(conn_str)
    sql_query = """
        SELECT DISTINCT 
            day_key,
            movie_key,
            imdb_rank,
            gross_earnings,
            imdb_rating,
            metascore_rating,
            num_votes,
            timestamp
        FROM
            movie_performance_staging
        {0}
        """

    movie_key_list = get_movie_key_for_delta(conn_str)

    if movie_key_list:
        formatted_sql_query = sql_query.format("WHERE movie_key IN (" + ",".join(get_movie_key_for_delta(conn_str)) + ")")
        populated_fact_staging_df = pd.read_sql_query(formatted_sql_query, con=engine)
        util.ingest_df_into_sql(populated_fact_staging_df, conn_str, "movie_performance_fact", "append")
        log.info("Movie performance fact table populated!")
    else:
        log.warning("No new facts populated")


def populate_schema(conn_str):
    create_new_dim_values(conn_str)
    populate_day_key_staging_table(conn_str)
    populate_movie_key_staging_table(conn_str)
    populate_movie_fact_table(conn_str)
