import os

from web import create_app

json_path = 'data/json/parsed_tv_shows.csv'
db_path = f'../data/sqlite/{os.path.basename(json_path)}.db'
app = create_app(json_path, db_path)


if __name__ == '__main__':
    app.run(debug=True)
