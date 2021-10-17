import csv
import os
import sys

import ujson as ujson


class CsvParser(object):
    def __init__(self, csv_fname):
        self._csv_fname = csv_fname
        self._old_col_names = []
        self._col_val_freq = {}
        self._movies = []

    def index(self):
        with open(self._csv_fname, newline='', encoding="utf8") as csvfile:
            spamreader = csv.reader(csvfile)
            for movie_row in spamreader:
                if len(self._old_col_names) == 0:
                    self._old_col_names = movie_row
                else:
                    movie = CsvParser._parse_movie_data({
                        col_name: movie_row[col_id]
                        for col_id, col_name in enumerate(self._old_col_names)
                    })
                    self._verify_movie(movie)
                    self._movies.append(movie)

    @classmethod
    def _parse_movie_data(cls, movie_data):
        # id, title, year, ?age+
        # ?rating_imdb, rating_rotten_tomatoes
        # is_on_netflix, is_on_hulu, is_on_prime_video, is_on_disney+
        output = {}
        output['id'] = movie_data['ID']
        output['title'] = movie_data['Title']
        output['year'] = int(movie_data['Year'])
        output['age'] = {
            '18+': 18,
            '16+': 16,
            '13+': 13,
            '7+': 7,
            'all': 0,
            '': None,
        }[movie_data['Age']]

        output['rating_imdb'] = \
            float(movie_data['IMDb'].split('/')[0]) * 10 \
                if len(movie_data['IMDb']) > 0 else None
        output['rating_rotten_tomatoes'] = \
            float(movie_data['Rotten Tomatoes'].split('/')[0])

        output['is_on_netflix'] = int(movie_data['Netflix'])
        output['is_on_hulu'] = int(movie_data['Hulu'])
        output['is_on_prime_video'] = int(movie_data['Prime Video'])
        output['is_on_disney'] = int(movie_data['Disney+'])

        return output

    def _verify_movie(self, movie):
        for k, v in movie.items():
            if k not in self._col_val_freq:
                self._col_val_freq[k] = {}
            if v not in self._col_val_freq[k]:
                self._col_val_freq[k][v] = 0
            self._col_val_freq[k][v] += 1
            if k in ['id', 'title'] and self._col_val_freq[k][v] > 1:
                raise Exception(f"Movie {k} appears twice:\n{movie}")

    def dump_movies_to_file(self, fname):
        with open(fname, 'w') as output_file:
            ujson.dump(self._movies, output_file, indent=4)
            output_file.write('\n')

    def dump_stats_to_file(self, fname):
        stats = self._col_val_freq.copy()
        del stats['id']
        del stats['title']
        with open(fname, 'w') as output_file:
            ujson.dump(stats, output_file, indent=4)
            output_file.write('\n')


def load_movies(fname):
    with open(fname, 'r') as input_file:
        movies = ujson.load(input_file)
        columns = list(movies[0].keys())
        return columns, movies


if __name__ == '__main__':
    input_fname = sys.argv[1]
    index_builder = CsvParser(input_fname)
    index_builder.index()

    output_path_for_movies = f'data/json/parsed_{os.path.basename(input_fname)}'
    output_path_for_stats = f'data/json/stats_{os.path.basename(input_fname)}'
    index_builder.dump_movies_to_file(output_path_for_movies)
    index_builder.dump_stats_to_file(output_path_for_stats)
    load_movies(output_path_for_movies)
