import main
import requests
import requests
import mimetypes
import re
import arrow

from guessit import guessit
# if guessit gives issues, try https://github.com/platelminto/parse-torrent-title
import arrow
import sys
from loguru import logger

from furl import furl
import time
from bs4 import BeautifulSoup

import marshmallow as ma

import typesense

from num2words import num2words
import urllib
import re
import os


class OMDBError(Exception):
    pass


logger.remove()
logger.add(sys.stderr, level="WARNING")

typesense_movies_schema = {
    'name': 'movies',
    'fields': [
        {'name': 'id', 'type': 'string'},   # same as file url : unique
        {'name': 'source_post', 'type': 'string', 'facet': True},
        {'name': 'directory_url', 'type': 'string', 'facet': True},
        {'name': 'screen_size', 'type': 'string', 'facet': True, 'optional': True},
        {'name': 'imdb_id', 'type': 'string'},
        {'name': 'title', 'type': 'string'},

        {'name': 'imdb_rating', 'type': 'float'},
        {'name': 'year', 'type': 'int32', 'optional': True, 'facet': True},

        {'name': 'genres', 'type': 'string[]', 'facet': True},
        {'name': 'director', 'type': 'string', 'facet': True},
        {'name': 'imdb_url', 'type': 'string'},

        {'name': 'language', 'type': 'string', 'facet': True,
            'optional': True},    # TODO: make this an array

        {'name': 'last_seen', 'type': 'int64'}
    ],
    'default_sorting_field': 'imdb_rating'
}

typesense_client = typesense.Client({
    'nodes': [{
        'host': 'zcwkae9v3yg17x6sp-1.a1.typesense.net',
        'port': '443',
        'protocol': 'https'
    }],
    'api_key': os.getenv("TYPESENSE_API_KEY"),
    'connection_timeout_seconds': 2
})

try:
    typesense_client.collections.create(typesense_movies_schema)
    logger.debug("Created 'movies' collection.")
except typesense.exceptions.ObjectAlreadyExists:
    logger.debug("Collection 'movies' already exist.")


class MovieSchema(ma.Schema):
    id = ma.fields.Str()    # based on url
    imdb_id = ma.fields.Str()
    title = ma.fields.Str()
    director = ma.fields.Str()
    file_url = ma.fields.URL()
    screen_size = ma.fields.Str()
    year = ma.fields.Int()
    genres = ma.fields.List(ma.fields.Str())
    imdb_rating = ma.fields.Float()  # 'N/A' will be -1...
    imdb_url = ma.fields.URL()

    last_seen = ma.fields.Int()

    # don't do things like cast etc -- if one day, ppl want to search by actor, first pass by tMDB and then retrieve info by title / id


def fetch_movie_data(href, fn):
    movie_info = guessit(fn)

    if movie_info['type'] == 'movie':
        # first get info from oMDB
        omdb_params = {
            'apikey': os.getenv("OMDB_API_KEY"),
            't': movie_info.get('title'),
        }

        if movie_info.get('year'):
            omdb_params['y'] = movie_info.get('year')

        omdb_data = requests.get(
            'http://www.omdbapi.com/', params=omdb_params).json()

        # params = {
        #     'api_key': 'e5b35c2691231b470b566875742d94c9',
        #     'query': movie_info.get('title'),
        #     'year': movie_info.get('year')
        # }

        # try:
        #     tmdb_data = requests.get('https://api.themoviedb.org/3/search/movie', params=params).json()['results'][0]
        # except IndexError:
        # # rate limited :(
        #     omdb_params = {
        #         'apikey': '898999fc',
        #         't': movie_info.get('title'),
        #         'y': movie_info.get('year')
        #     }

        #     # logger.error(f"No results found for {fn} (extracted title ={movie_info.get('title')})")

        #     return {'file_url': href}

        # params = {'api_key': params['api_key'], 'append_to_response': 'credits,external_ids'}
        # extra_data = requests.get(f'https://api.themoviedb.org/3/movie/{tmdb_data["id"]}', params=params).json()

        # tmdb_data['tmdb_id'] = tmdb_data['id']
        # tmdb_data['imdb_id'] = extra_data['external_ids']['imdb_id']

        # movie_data = {**tmdb_data, **extra_data}

        if omdb_data.get('Error'):
            if omdb_data.get('Error') == "Movie not found!":
                logger.warning(f'Movie not found: {href}')
                title = movie_info['title']
                for s in title.split():
                    if s.isdigit():
                        title.replace(s, num2words(s)).title()
                if title != movie_info['title']:
                    logger.debug(f'Trying once more with {title}')
                    omdb_params['t'] = title
                    omdb_data = requests.get(
                        'http://www.omdbapi.com/', params=omdb_params).json()
                    if omdb_data.get('Error'):
                        # TODO: send issue to Sentry
                        # raise OMDBError(movie_data.get('Error'))
                        return None
                else:
                    return None
            else:
                logger.error(f'{omdb_data} for {href}')
                return None

        movie_data = {**omdb_data}

        try:
            movie = MovieSchema().load(
                {'imdb_rating': movie_data['imdbRating']})
        except ma.ValidationError as e:
            if 'imdb_rating' in e.messages:
                del movie_data['imdbRating']

        try:
            movie = MovieSchema().load(dict(
                id=href,
                imdb_id=movie_data['imdbID'],
                title=movie_data['Title'],
                director=movie_data['Director'],
                file_url=furl(href).url,
                genres=movie_data['Genre'].split(', '),
                imdb_rating=movie_data.get('imdbRating', -1),
                imdb_url=f"https://www.imdb.com/title/{movie_data['imdbID']}/reference",
                last_seen=arrow.now().timestamp
            ))
            if movie_data['Year'].isnumeric():
                # Can be null, but ma doesn't accept null. So make it optional.
                movie['year'] = int(movie_data['Year'])

            if movie_info.get('screen_size'):
                movie['screen_size'] = movie_info.get('screen_size')

            if not isinstance(movie.get('language', None), str):
                logger.warning(movie_info)
                try:
                    movie['language'] = ','.join([language.alpha3 for language in movie.get(
                        'language')]) if movie.get('language') else ''
                except TypeError:   # Not iterable, so single language
                    movie['language'] = movie.get(
                        'language').alpha3 if movie.get('language') else ''

            return movie
        except ma.ValidationError as e:
            # TODO: missing screen size also gives an error :/
            logger.warning(f"No valid movie information found for {href}")
            logger.error(e)
            return None
        except KeyError as e:
            logger.error(e)
            logger.info(f"{href} returned {movie_data}")
            # TODO: send to Sentry


def parse_txt(fn, path_must_start_with='', only_latin_chars=False, exclude_substr=None):
    with open(fn, 'r') as f:
        href = f.readline().strip()
        while href:
            if exclude_substr and exclude_substr not in href:
                if href.startswith(path_must_start_with):
                    logger.debug(f"Parsing {href}")
                    link_type, _ = mimetypes.guess_type(href)
                    logger.debug(f"Link type is {link_type}")
                    if link_type:
                        # If link_type is None, it's in any case not a video
                        if 'video' in link_type:
                            fn = furl(href).path.segments[-1]
                            fn = urllib.parse.unquote(fn)
                            if only_latin_chars:
                                fn = re.sub(r'[^a-zA-Z0-9./]', '', fn)
                            movie = fetch_movie_data(
                                urllib.parse.unquote(href), fn)
                            if movie:
                                if movie.get('title') and movie.get('year'):
                                    logger.debug(
                                        f"Found {movie['title']} ({movie.get('year')})")
                                elif movie.get('title') and not movie.get('year'):
                                    logger.warning(
                                        f"Found {movie['title']} but no year found for {movie['file_url']}")
                                yield movie
                            else:
                                logger.debug(f"No movie found; got {movie}")
                                yield None
            href = f.readline().strip()


def parse_html_directory(base_url, url=None, visited=None):
    visited = visited if visited else []
    url = base_url if url is None else url
    html_doc = requests.get(url, verify=False).text
    soup = BeautifulSoup(html_doc, 'html.parser')
    for idx, a in enumerate([a for a in soup.find_all('a') if a['href'] not in visited]):
        href = a['href']
        logger.debug(f"Parsing {href}")
        if href != '../':  # Don't go back :)
            visited.append(href)
            if not(furl(href).scheme) and not (furl(href).netloc):
                # create full link
                href = furl(base_url).join(href).url

            if furl(href).netloc != furl(base_url).netloc:
                logger.debug(f"Not following external link {href}")
            else:
                # is it a path?
                if href[-1] == '/':
                    yield from parse_html_directory(base_url, href, visited)
                else:
                    # first, check if this is a video?
                    link_type, _ = mimetypes.guess_type(href)
                    logger.debug(f"Link type is {link_type} at {href}")
                    if link_type:
                        # If link_type is None, it's in any case not a video
                        if 'video' in link_type:
                            fn = furl(href).path.segments[-1]
                            movie = fetch_movie_data(
                                href, fn)
                            if movie:
                                if movie.get('title') and movie.get('year'):
                                    logger.debug(
                                        f"Found {movie['title']} ({movie.get('year')})")
                                elif movie.get('title') and not movie.get('year'):
                                    logger.warning(
                                        f"Found {movie['title']} but no year found for {movie['file_url']}")
                                yield movie
                            else:
                                logger.debug(
                                    f"No movie found for {fn} at {href}")
                                yield None


def output_movies(movies, fn=None, sort_by="rating"):
    fn = fn if fn else 'movies.md'
    header = "|Title|IMDB rating|\n| ------------ | ------------ |\n"
    with open(fn, 'w') as f:
        f.write(header)
        for movie in sorted(movies, key=lambda x: x.get('imdb_rating'), reverse=True):
            if movie.get('title'):
                # try:
                line_to_add = f"""|[{movie['title']}]({movie['file_url']}) ({f"{movie['year']}, " if movie.get('year') else ""}{movie['director']}) *{", ".join(movie['genres'])}* | [{movie['imdb_rating']}]({movie['imdb_url']})"""
    #                     line_to_add = f"|**{omdb_data['Title']}** ({omdb_data['Year']}, {omdb_data['Director']}) [💾]({omdb_data['file_url']}) | [{omdb_data['imdbRating']}]({omdb_data['imdbUrl']}) "
            #     except (KeyError, ValueError):
            #         line_to_add = f"|{movie['file_url']} | [💾]({movie['file_url']}) |"
            else:
                line_to_add = f"|{movie['file_url']} | [💾]({movie['file_url']}) |"

            f.write(f'{line_to_add}\n')


def bulk_index_movies(movies):
    logger.debug(f'Indexing {len(movies)} movies.')
    valid_movies = [m for m in movies if m['title']]
    typesense_client.collections['movies'].documents.import_(
        valid_movies, {'action': 'upsert'})


def index_single_movie(movie, open_directory_url, reddit_source):
    movie['source_post'] = reddit_source
    movie['directory_url'] = open_directory_url
    typesense_client.collections['movies'].documents.upsert(movie)

# This one is a tough one: http://anilist1.ir/?dir=Movie/1978/Woman%20Chasing%20the%20Butterfly%20of%20Death


# sources = [
#     {
#         'r': 'https://www.reddit.com/r/opendirectories/comments/ka7vl5/fast_french_movie_server/',
#         'd': 'https://87.98.218.57/'
#     },
#     {
#         'r': 'https://www.reddit.com/r/opendirectories/comments/kcs28h/http18519178214/',
#         'd': 'http://185.191.78.214/'
#     }]

# movies = []

# for source in sources:
#     for movie in main.parse_html_directory(source['d']):
#         if movie:
#             movies.append(movie)
#             try:
#                 main.index_single_movie(
#                     movie, open_directory_url=source['d'], reddit_source=source['r'])
#             except Exception as e:
#                 logger.error(f'Error {e} encountered with {movie}')
#     main.output_movies(movies, fn=f'{source["r"]}.md')
#     print(len(movies))
