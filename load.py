import pandas as pd
from sqlalchemy import create_engine

import util


def get_new_time_dim(conn_str):
    engine = create_engine(conn_str)
    curr_time_dim_df = pd.read_sql_query("""
        SELECT DISTINCT 
            capture_date,
            month_code,
            quarter_code,
            year
        FROM
            day_dim
    """, con=engine)

    curr_staging_time_df = pd.read_sql_query("""
        SELECT DISTINCT 
            capture_date,
            month_code,
            quarter_code,
            year
        FROM
            movie_performance_staging
    """, con=engine)

    new_time_dim_df = curr_staging_time_df[~curr_staging_time_df.index.isin(curr_time_dim_df.index)]

    return new_time_dim_df


def get_new_movie_dim(conn_str):
    engine = create_engine(conn_str)
    curr_time_dim_df = pd.read_sql_query("""
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

    curr_staging_time_df = pd.read_sql_query("""
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
    """, con=engine)

    new_time_dim_df = curr_staging_time_df[~curr_staging_time_df.index.isin(curr_time_dim_df.index)]

    return new_time_dim_df


def create_new_dim_tables(conn_str):
    util.ingest_df_into_sql(get_new_time_dim(conn_str), conn_str, "day_dim", "append")
    util.ingest_df_into_sql(get_new_movie_dim(conn_str), conn_str, "movie_dim", "append")


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

    print("Done populating day_key")


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

    print("Done populating movie_key")


def populate_movie_fact_table(conn_str):
    engine = create_engine(conn_str)
    populated_fact_staging_df = pd.read_sql_query("""
            SELECT DISTINCT 
                day_key,
                movie_key,
                gross_earnings,
                imdb_rating,
                metascore_rating,
                num_votes,
                timestamp
            FROM
                movie_performance_staging
        """, con=engine)

    util.ingest_df_into_sql(populated_fact_staging_df, conn_str, "movie_performance_fact", "append")


def populate_schema(conn_str):
    create_new_dim_tables(conn_str)
    populate_day_key_staging_table(conn_str)
    populate_movie_key_staging_table(conn_str)
    populate_movie_fact_table(conn_str)
