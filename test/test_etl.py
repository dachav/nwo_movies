import datetime
import unittest

import pandas as pd
import sqlalchemy.exc

import config
import extract
import load
import transform


class ExtractTestCase(unittest.TestCase):
    def test_genre_urls(self):
        self.assertIsInstance(extract.get_category_urls(config.IMDB_GENRE_URL), dict)

    def test_get_movie_info(self):
        self.assertIsInstance(extract.get_movie_info("Test", "http://url.com", datetime.datetime.utcnow()), list)


class TransformTestCase(unittest.TestCase):
    def test_get_quarter(self):
        self.assertTrue(transform.get_quarter_code({"month_code": 4}), 2)
        self.assertTrue(transform.get_quarter_code({"month_code": 1000}), 4)

    def test_clean_release_year(self):
        self.assertTrue(transform.clean_release_year({"release_year": "// 2012"}), 2012)
        self.assertTrue(transform.clean_release_year({"release_year": "2d?d43r3"}), 2433)

    def test_transform_staging_table(self):
        df = pd.DataFrame.from_records([{'title': 'test'
                                            , 'url': 'test/test/test'
                                            , 'release_year': '2001'
                                            , 'mpaa_rating': 'test'
                                            , 'runtime_minutes': '100 min'
                                            , 'genres': 'test'
                                            , 'imdb_rating': '9.0'
                                            , 'metascore_rating': '9'
                                            , 'actors': 'test'
                                            , 'directors': 'test'
                                            , 'summary': 'test'
                                            , 'num_votes': '100'
                                            , 'gross_earnings': '100'
                                            , 'timestamp': '2021-11-19 15:43:42.515297'
                                            , 'imdb_rank': 'test'}])
        self.assertIsInstance(transform.transform_staging_table(df), transform.MoviePerformanceStaging)


class LoadTestCase(unittest.TestCase):
    def test_get_movie_key_for_delta(self):
        self.assertIsInstance(load.get_movie_key_for_delta(config.CONNECTION_STRING), list)


if __name__ == '__main__':
    unittest.main()
