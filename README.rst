Dump of a script I wrote to run through an open directory (typically found on /r/opendirectories) and

* Extract the movie name, year and resolution
* Run it through http://www.omdbapi.com/ to fetch all details (director, genres, IMDB rating, ...)
* Save it to a https://typesense.org/ node so that the movies are searchable. The front-end for that is at https://github.com/LaundroMat/odMovieSearch

Usage:

Install dependencies with poetry.

I typically use a Jupyter notebook to run things. In `Reddit - Opendirectory scraper.ipynb` there are a few scripts to muck around with things.

The easiest way to scrape a directory is with the first script in the above mentioned notebook.

* First, you'll need to get or generate a .txt file with all files found on the server with https://github.com/KoalaBear84/OpenDirectoryDownloader (/user/KoalaBear84/ typically provides these in the comments of an OD post on reddit too)
* `main.parse_txt()` extracts the movie name for each file and runs it through OMDB
* `main.output_movies()` saves the movies as a markup file, ordered by IMDB rating
* `main.index_single_movie()` saves each movie to the typesense index.

This code is provided as-is. Do with it what you want. If you need support, open an issue, but I can't guarantee I'll be able to help.