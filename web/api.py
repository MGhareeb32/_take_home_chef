from flask import Blueprint, render_template, request, flash, jsonify

from movie_store import Condition
from web import db

api = Blueprint('views', __name__)


def _pretty_column_names():
    return {
        'id': 'ID', 'title': 'Title', 'year': 'Year Released', 'age': 'Rating',
        'rating_imdb': 'IMDb Rating', 'rating_rotten_tomatoes': 'Rotten Tomatoes Rating',
        'is_on_netflix': 'Netflix', 'is_on_hulu': 'Hulu', 'is_on_prime_video': 'Prime Video',
        'is_on_disney': 'Disney+',
    }


def _make_movie_tooltip(movie):
    tooltip = []
    col_names = _pretty_column_names()
    for k in ['is_on_netflix', 'is_on_hulu', 'is_on_prime_video', 'is_on_disney']:
        if movie[k]:
            tooltip.append(col_names[k])
    return 'Available on: ' + ', '.join(tooltip)


@api.route('/', methods=['GET'])
def home():
    # note_text = request.form.get('note_text')
    search_text = request.values.get('search_text')
    if search_text is not None:
        query_conds = (
            Condition('title', 'like', search_text),
        )
    else:
        query_conds = ()
    try:
        page = int(request.values.get('page'))
    except Exception as _:
        page = 1

    movies = db.query(query_conds, sort_by=None, page_offset=page-1, page_size=6)
    for movie in movies:
        movie['tooltip'] = _make_movie_tooltip(movie)
    response = {
        'pretty_column_names': _pretty_column_names(),
        'movies': movies,
    }
    return render_template("index.html", response=response)
