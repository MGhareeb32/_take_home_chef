import os.path
import sqlite3
from collections import defaultdict

import csv_parser


class MoviesDb(object):
    def __init__(self, db_file_path, source_json_path=None):
        self.db_file_path = db_file_path
        self.source_json_path = source_json_path

    def _connection(self):
        return sqlite3.connect(self.db_file_path)

    def _create_db_if_needed(self):
        cur = self._connection().cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        if len(cur.fetchall()) == 0:
            print(f'Creating movies table...')
            # id, title, year, ?age
            # ?rating_imdb, rating_rotten_tomatoes
            # is_on_netflix, is_on_hulu, is_on_prime_video, is_on_disney+
            cur.execute('''
            CREATE TABLE movies (
                id INTEGER, title TEXT, year REAL, age REAL,
                rating_imdb REAL, rating_rotten_tomatoes REAL,
                is_on_netflix INTEGER, is_on_hulu INTEGER, is_on_prime_video INTEGER, is_on_disney INTEGER)
            ''')
            self._connection().commit()

    def _populate_db_if_needed(self):
        cur = self._connection().cursor()
        cur.execute("SELECT count() FROM movies;")
        if cur.fetchone()[0] == 0:
            print(f'No records in movies table.')
            if self.source_json_path is None:
                print(f'  No source file given to populate the table. Quitting...')
                exit()
            print(f'  Populating from {self.source_json_path}...')
            _, movies = csv_parser.load_movies(self.source_json_path)
            for movie in movies:
                query = f'''
                INSERT INTO movies VALUES (
                    ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, ?, ?
                )'''
                cur.execute(
                    query,
                    (
                        movie['id'], movie['title'], movie['year'], movie['age'],
                        movie['rating_imdb'], movie['rating_rotten_tomatoes'],
                        movie['is_on_netflix'], movie['is_on_hulu'],
                        movie['is_on_prime_video'], movie['is_on_disney']
                    )
                )
            self._connection().commit()
            print(f'  Inserted {len(movies)} rows into movies table.')

    def connect(self):
        print(f'Connecting to database in {self.db_file_path}...')
        try:
            os.makedirs(os.path.dirname(self.db_file_path), exist_ok=True)
            con = sqlite3.connect(self.db_file_path)
            self._create_db_if_needed()
            self._populate_db_if_needed()
        except Exception as e:
            os.remove(self.db_file_path)
            raise e
        print(f'Database connection ready.')

        return con

    def query(self, conds, sort_by=None, page_offset=0, page_size=10):
        conds_sql = ' AND '.join([
            str(cond) for cond in conds
            if cond.condition_type is not None
        ])
        if len(conds_sql) > 0:
            conds_sql = f'WHERE {conds_sql}'
        if sort_by is None:
            sort_by = 'id'

        sql = f'''
        WITH movies_ranked AS (
             SELECT
                *,
                RANK() OVER (ORDER BY {sort_by}) rank
            FROM movies
            {conds_sql}
        )
        SELECT {', '.join(self.column_names())}
            FROM movies_ranked
        WHERE rank > {page_offset * page_size}
        ORDER BY {sort_by}
        LIMIT {page_size};
        '''
        # print(sql)
        cur = self._connection().cursor()
        cur.execute(sql)
        results = cur.fetchall()
        print(f'    Found {len(results)} rows.')
        return [
            {
                self.column_names()[i]: self.fix_col_type(self.column_names()[i], v)
                for i, v in enumerate(movie)
            }
            for movie in results
        ]

    def column_names(self):
        return [
            'id', 'title', 'year', 'age',
            'rating_imdb', 'rating_rotten_tomatoes',
            'is_on_netflix', 'is_on_hulu', 'is_on_prime_video', 'is_on_disney',
        ]

    def fix_col_type(self, col, val):
        return defaultdict(lambda: lambda: val, {
            'year': lambda: int(val), 
            'rating_imdb': lambda: val / 10 if val is not None else '-',
        })[col]()


class Condition(object):
    TYPE_TO_SQL = {
        None: lambda c, v: None,
        '==': lambda c, v: f'{c} == {v}',
        '!=': lambda c, v: f'{c} <> {v}',
        '>=': lambda c, v: f'{c} >= {v}',
        '<=': lambda c, v: f'{c} <= {v}',
        '>': lambda c, v: f'{c} > {v}',
        '<': lambda c, v: f'{c} < {v}',
        'like': lambda c, v: f'{c} like \'%{v}%\'',
    }

    def __init__(self, col_name, condition_type=None, condition_value=None):
        self.col_name = col_name
        self.condition_type = condition_type
        self.condition_value = condition_value

    def __str__(self):
        return f'{Condition.TYPE_TO_SQL[self.condition_type](self.col_name, self.condition_value)}'


if __name__ == '__main__':
    json_path = 'data/json/parsed_tv_shows.csv'
    db_path = f'data/sqlite/{os.path.basename(json_path)}.db'
    db = MoviesDb(db_path, json_path)
    db.connect()
    print(db.column_names())

    query_conds = (
        Condition('id', '!=', 1),
        Condition('title', 'like', 'a'),
        Condition('year', '>=', 2000),
        Condition('age', '>=', 18),
        Condition('rating_imdb', '>', 40),
        Condition('rating_rotten_tomatoes'),
        Condition('is_on_netflix', '==', 1),
        Condition('is_on_hulu'),
        Condition('is_on_prime_video'),
        Condition('is_on_disney'),
    )
    print('TESTING 10 ITEMS')
    results = db.query(query_conds, sort_by=None, page_offset=0, page_size=10)
    for movie in results:
        print(movie)
    print('TESTING SAME ITEMS BUT PAGINATED')
    for i in range(5):
        results = db.query(query_conds, sort_by=None, page_offset=i, page_size=2)
        for movie in results:
            print(movie)
    print('TESTING EMPTY CONDITION')
    results = db.query([], sort_by=None, page_offset=0, page_size=10)
    for movie in results:
        print(movie)
