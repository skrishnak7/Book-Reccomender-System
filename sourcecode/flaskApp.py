from flask import Blueprint
import csv
main = Blueprint('main', __name__)
import json
from engine import RecommendationEngine
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from flask import Flask, request


@main.route("/<int:user_id>/suggested-count/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id, count)
    y = top_ratings
    for row in y:
        print (row)
    page = ''
    page = '<table> <tr> <th>Ratings</th> <th>Title</th>  </tr>'
    for row1 in y:
        print (row1)
        page = page + '<tr> <td> %s</td> <td>%s</td></tr>' % (row[1], row[0],)
    page += '</table>'
    return json.dumps(top_ratings)



@main.route("/<int:user_id>/book_id/<int:book_id>", methods=["GET"])
def book_ratings(user_id, book_id):
    logger.debug("User %s rating requested for book %s", user_id, book_id)
    ratings = recommendation_engine.get_ratings_for_book_ids(user_id, [book_id])
    return json.dumps(ratings)


@main.route("/<int:user_id>/ratings", methods=["POST"])
def add_ratings(user_id):

    ratings_list = request.form.keys()[0].strip().split("\n")
    ratings_list = map(lambda x: x.split(","), ratings_list)
    ratings = map(lambda x: (user_id, int(x[0]), float(x[1])), ratings_list)
    recommendation_engine.add_ratings(ratings)
    return json.dumps(ratings)


@main.route("/")
def index():
    page = ''
    page = '<table> <tr> <th>Ratings</th> <th>Title</th> <th>BookCover</th> </tr>'
    file = 'toprated.csv'
    with open(file) as f:
        r = csv.reader(f, delimiter=',')
        next(r)
        for row in r:
            page = page + '<tr> <td> %s</td> <td>%s</td> <td>  <img src=%s alt="Mountain View" style="width:304px;height:228px;"> </td></tr>' % (
            row[2], row[3], row[4],)
    page += '</table>'
    return page


def create_app(spark_context, dataset_path):
    global recommendation_engine
    recommendation_engine = RecommendationEngine(spark_context, dataset_path)
    app = Flask(__name__)
    app.register_blueprint(main)
    return app