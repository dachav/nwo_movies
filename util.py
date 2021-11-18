import glob
import os

import pandas as pd
from sqlalchemy import create_engine


def read_all_csv_to_df(path):
    all_files = glob.glob(path + "/*.csv.gz")
    li = []

    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)

    return pd.concat(li, axis=0, ignore_index=True)


def ingest_append_data(df, conn_str, table_name):
    engine = create_engine(conn_str)
    df.to_sql(table_name, engine, if_exists='append', index=False)


def run_crud_operation(conn_str, stmt):
    engine = create_engine(conn_str)
    conn = engine.connect()
    conn.execute(stmt)
    conn.close()


def remove_files(path):
    files = glob.glob(path)
    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print("Error: %s : %s" % (f, e.strerror))
