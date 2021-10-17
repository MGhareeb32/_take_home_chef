from flask import Flask
from movie_store import MoviesDb

db = None


def create_app(json_path, db_path):
    global db
    db = MoviesDb(db_path, json_path)
    db.connect()

    app = Flask(__name__)
    app.config['SECRET_KEY'] = '123456789'
    app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{db_path}'

    from .api import api
    app.register_blueprint(api, url_prefix='/')

    return app
