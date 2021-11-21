import config
import util


def check_empty_file(df):
    if df.empty:
        print('File is empty')
        return False
    return True


def check_missing_cols(df):
    # variable to hold validation value default true
    pass_validation = True
    # same group by aggregation used in the main app in order to get the correct row counts
    df = df.groupby(['title', 'url', 'release_year', 'mpaa_rating', 'runtime_minutes',
                     'genres', 'imdb_rating', 'metascore_rating', 'actors', 'directors',
                     'summary', 'num_votes', 'gross_earnings', 'file_name',
                     'timestamp'], dropna=False).agg({'imdb_rank': ','.join}).reset_index()

    # iterate through columns in raw file df looking for nulls
    for column in df.columns:
        count_missing = df[column].isnull().sum()
        if count_missing > 0:
            result = util.run_crud_operation(config.CONNECTION_STRING,
                                             "SELECT COUNT(*) FROM movie_performance_staging WHERE "
                                             "%s IS NULL " % column).fetchone()

            # comparing nulls in staging to nulls in raw file df
            if result[0] != count_missing:
                print("column: '%s' has a different number of null values comparing the raw file (%s) to staging (%s)"
                      % (column, count_missing, result[0]))
                pass_validation = False

    return pass_validation


def check_unique_values(df):
    # dropping duplicate movies via url, a synthetic key
    unique_movies = len(df["url"].drop_duplicates())
    # get count values in staging which represent unique movies
    result = util.run_crud_operation(config.CONNECTION_STRING, "SELECT COUNT(*) FROM movie_performance_staging") \
        .fetchone()
    # checking if unique movie counts are the same in raw file as in staging
    if result[0] != unique_movies:
        print("Number of unique records in the raw file (%s) does not match staging (%s)" % (unique_movies, result[0]))
        return False
    return True


def get_num_delta_records(file_names_str):
    # query checks delta between staging and fact table and doesnt include current staging data
    delta_query = """
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
                    WHERE file_name NOT IN ({0})
                """
    formatted_delta_query = delta_query.format(file_names_str)
    result = util.run_crud_operation(config.CONNECTION_STRING, formatted_delta_query).fetchall()
    # return record count
    return len(result)


def check_delta_counts(df):
    # get file names for raw files in the folder
    file_names = df['file_name'].drop_duplicates().tolist()
    # joining string with quotes around to support querying
    file_names_str = ', '.join(["'{}'".format(value) for value in file_names])
    # getting count of records in the fact table that are from the files in the raw folder
    result = util.run_crud_operation(config.CONNECTION_STRING, "SELECT COUNT(*) FROM movie_performance_fact WHERE "
                                                               "file_name IN (%s) " % file_names_str).fetchone()
    # calling delta function
    num_delta_records = get_num_delta_records(file_names_str)
    # checking the records added to fact match the number of records in delta
    if num_delta_records != result[0]:
        print("The number of delta records (%s) does not match new records in fact table (%s)"
              % (num_delta_records, result[0]))
        return False

    return True


def main():
    # read the files in raw_data_path
    df = util.read_all_csv_to_df(config.RAW_DATA_PATH)

    # written to allow all functions to be called
    validation_passed = True
    validation_passed = check_empty_file(df) and validation_passed
    validation_passed = check_missing_cols(df) and validation_passed
    validation_passed = check_unique_values(df) and validation_passed
    validation_passed = check_delta_counts(df) and validation_passed

    if validation_passed:
        print("Validation passed")
    else:
        print("Validation failed")


if __name__ == '__main__':
    main()
