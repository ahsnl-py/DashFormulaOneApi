from flask import Flask
from flask.json import jsonify
from src.constants.http_status_codes import HTTP_404_NOT_FOUND, HTTP_500_INTERNAL_SERVER_ERROR
import os
from src.dashfone_home_page import stats, db, cors

def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)

    username = 'vhqehuwdwfuyyl'
    password = '45b56a2442d08dddebcd39177a2048da8054b203ad75e1916fca9cf16980a05b'
    host = 'ec2-3-248-87-6.eu-west-1.compute.amazonaws.com'
    port = 5432
    dbname = 'ddncsklomh2hu2'

    if test_config is None:
        app.config.from_mapping(
            SECRET_KEY=os.environ.get("SECRET_KEY"),
            # SQLALCHEMY_DATABASE_URI=os.environ.get("SQLALCHEMY_DATABASE_URI"),
            SQLALCHEMY_DATABASE_URI=f"postgresql://{username}:{password}@{host}:{port}/{dbname}",
            SQLALCHEMY_TRACK_MODIFICATIONS=os.environ.get("SQLALCHEMY_TRACK_MODIFICATIONS")
        )   
    else:
        app.config.from_mapping(test_config)

    db.app = app
    db.init_app(app)
    cors.init_app(app)
    app.register_blueprint(stats)

    @app.errorhandler(HTTP_404_NOT_FOUND)
    def handle_404(e):
        return jsonify({'error': 'Not found'}), HTTP_404_NOT_FOUND

    @app.errorhandler(HTTP_500_INTERNAL_SERVER_ERROR)
    def handle_500(e):
        return jsonify({'error': 'Something went wrong, we are working on it'}), HTTP_500_INTERNAL_SERVER_ERROR

    return app