import config
import util


def check_missing_cols(df):
    for column in df.columns:
        count_missing = df[column].isnull().sum()
        if count_missing > 0:
            print("%s has %s missing value(s)" % (column, count_missing))


def check_empty_file(df):
    if df.empty:
        print('File is empty')


def check_duplicates(df):
    exact_dupes = len(df) - len(df.drop_duplicates())
    movie_dupes = len(df["url"].drop_duplicates())

    if exact_dupes > 0:
        print("%s exact duplicates found" % exact_dupes)

    if movie_dupes > 0:
        print("%s unique movies found" % movie_dupes)


def main():
    df = util.read_all_csv_to_df(config.RAW_DATA_PATH)
    check_empty_file(df)
    check_missing_cols(df)
    check_duplicates(df)


if __name__ == '__main__':
    main()
