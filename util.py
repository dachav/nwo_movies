import glob
import logging
import os
import shutil

import pandas as pd
from sqlalchemy import create_engine, exc


def read_all_csv_to_df(path):
    all_files = glob.glob(path + "/*.csv.gz")
    li = []

    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)

    return pd.concat(li, axis=0, ignore_index=True)


def ingest_df_into_sql(df, conn_str, table_name, action_if_exists):
    try:
        engine = create_engine(conn_str)
        df.to_sql(table_name, engine, index=False)
    except exc.SQLAlchemyError as e:
        logging.error("df: %s connection string: %s table name: %s action: %s Error: %s"
                      % (df, conn_str, table_name, action_if_exists, e))


def run_crud_operation(conn_str, stmt):
    try:
        engine = create_engine(conn_str)
        conn = engine.connect()
        conn.execute(stmt)
        conn.close()
    except exc.SQLAlchemyError as e:
        logging.error("connection string: %s statement: %s Error: %s" % (conn_str, stmt, e))
        raise


def archive_old_files(source_dir, target_dir):
    try:
        file_names = os.listdir(source_dir)

        for file_name in file_names:
            shutil.move(os.path.join(source_dir, file_name), target_dir)
    except (shutil.Error, OSError) as e:
        logging.error("source: %s target: %s Error: %s" % (source_dir, target_dir, e.strerror))
        raise


def create_folders_if_missing(paths):
    for path in paths:
        try:
            # Check whether the specified path exists or not
            is_exist = os.path.exists(path)

            if not is_exist:
                # Create a new directory because it does not exist
                os.makedirs(path)
                print("The new directory is created!")
        except OSError as e:
            logging.error("path: %s Error: %s" % (path, e.strerror))
            raise
