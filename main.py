import config
import extract
import logging_config
import transform
import load


def main():
    new_data_pull = input("\nExport new top 50 data?: (y or n) ")
    log.info("user input to export top 50: %s" % new_data_pull)
    if new_data_pull == 'y':
        extract.export_archived_file(config.IMDB_GENRE_URL, config.RAW_DATA_PATH, config.RAW_DATA_ARCHIVE_PATH)

    update_staging = input("\nUpdate staging table?: (y or n) ")
    log.info("user input to update staging: %s" % update_staging)
    if update_staging == 'y':
        transform.ingest_new_staging_data(config.RAW_DATA_PATH, config.CONNECTION_STRING)

    populate_schema = input("\nPopulate schema?: (y or n) ")
    log.info("user input to populate schema: %s" % populate_schema)
    if populate_schema == 'y':
        load.populate_schema(config.CONNECTION_STRING)


if __name__ == '__main__':
    log = logging_config.configure_logger("default", "logging.log")
    log.info("program start")
    main()
