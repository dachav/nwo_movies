import logging

import config
import extract
import util
import transform
import load


def ingest_new_staging_data():
    raw_data_df = util.read_all_csv_to_df(config.RAW_DATA_PATH)
    staging_table_df = transform.create_staging_table(raw_data_df)
    util.ingest_append_data(staging_table_df, config.CONNECTION_STRING, "movie_performance_staging")


def main():
    new_data_pull = input("Export new top 50 data?: (y or n) ")
    if new_data_pull == 'y':
        util.remove_files(config.RAW_DATA_PATH + "/*")
        extract.export_archived_file()

    update_staging = input("Update staging table?: (y or n) ")
    if update_staging == 'y':
        ingest_new_staging_data()

    populate_schema = input("Populate schema?: (y or n) ")
    if populate_schema == 'y':
        load.populate_schema(config.CONNECTION_STRING)


if __name__ == '__main__':
    logging.basicConfig(filename='./logs/logging.log', level=logging.INFO)
    logging.info('Started')
    main()
