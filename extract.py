from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

import movies


def get_category_urls():
    # Parse the html content
    category_url = "https://www.imdb.com/feature/genre?ref_=fn_asr_ge/"

    data = {}
    # Make a GET request to fetch the raw HTML content
    html_content = requests.get(category_url).text
    soup = BeautifulSoup(html_content, "lxml")
    div = soup.select("a[name=slot_right-4] + div")

    for a in div[0].find_all('a', href=True):
        data[a.text.strip()] = "".join(["https://www.imdb.com", a['href']])
    return data


def get_movie_info(top_category, url, timestamp):
    data = []
    # Make a GET request to fetch the raw HTML content
    html_content = requests.get(url).text
    soup = BeautifulSoup(html_content, "lxml")
    movie_list = soup.select("div .lister-item-content")

    # Iterate through each movie section
    for movie in movie_list:

        movie_info = {}
        header = movie.find('h3', class_="lister-item-header")

        movie_info["title"] = header.find('a').text
        movie_info["url"] = header.find('a')['href']
        movie_info["imdb_rank"] = {top_category: header.find('span', class_="lister-item-index").text.replace(".", "")}
        movie_info["release_year"] = header.find('span', class_="lister-item-year").text

        mpaa_rating = movie.find('span', class_="certificate")
        movie_info["mpaa_rating"] = None
        if mpaa_rating is not None:
            movie_info["mpaa_rating"] = mpaa_rating.text

        runtime_minutes = movie.find('span', class_="runtime")
        movie_info["runtime_minutes"] = None
        if runtime_minutes is not None:
            movie_info["runtime_minutes"] = runtime_minutes.text.strip()

        genres = movie.find('span', class_="genre")
        movie_info["genres"] = None
        if genres is not None:
            sorted_genre_list = sorted(genres.text.strip().split(", "))
            movie_info["genres"] = ", ".join(sorted_genre_list)

        nv_element = movie.select('span[name=nv]')
        movie_info["num_votes"] = None
        movie_info["gross_earnings"] = None
        if nv_element:
            movie_info["num_votes"] = nv_element[0]['data-value']
            if len(nv_element) > 1:
                movie_info["gross_earnings"] = nv_element[1]['data-value']

        bottom_row = movie.select('p:-soup-contains("Director")')

        directors_and_actors = bottom_row[0].select("p > a")
        actors = bottom_row[0].select("span ~ a")

        directors = list(set(directors_and_actors) - set(actors))

        sorted_actor_list = sorted([actor.text for actor in actors])
        sorted_director_list = sorted([director.text for director in directors])

        movie_info["actors"] = ", ".join(sorted_actor_list)
        movie_info["directors"] = ", ".join(sorted_director_list)

        summary = movie.select('div.ratings-bar + p')
        movie_info["summary"] = summary[0].text.strip()

        imdb_rating = movie.find('div', class_="ratings-imdb-rating")
        movie_info["imdb_rating"] = None
        if imdb_rating is not None:
            movie_info["imdb_rating"] = imdb_rating['data-value']

        metascore_rating = movie.find('span', class_="metascore")
        movie_info["metascore_rating"] = None
        if metascore_rating is not None:
            movie_info["metascore_rating"] = metascore_rating.text.strip()

        if movie_info is not None:
            data.append(movies.Movie(movie_info, timestamp))

    return data


def export_archived_file():
    # write a pandas dataframe to gzipped CSV file
    movies_list = []
    categories = get_category_urls()
    timestamp = datetime.utcnow()
    for k, v in categories.items():
        movies_list = movies_list + get_movie_info(k, v, timestamp)

    movies_df = pd.DataFrame.from_records([m.to_dict() for m in movies_list])
    file_created_time = datetime.now().strftime("%Y%m%d-%H%M%S")
    movies_df.to_csv(f"./raw_data/top50movies{file_created_time}.csv.gz", index=False, compression='gzip')
    print("Done!")
