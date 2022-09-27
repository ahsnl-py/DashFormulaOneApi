from flask import Flask
from flask.json import jsonify
from src.config import config
from src.constants.http_status_codes import HTTP_404_NOT_FOUND, HTTP_500_INTERNAL_SERVER_ERROR
import os
from src.api.dashfone_home_page import stats, db, cors
from src.api.dashfone_request_to_db import utils, db, cors

    
db_config = {
    'username': '',
    'password': '',
    'host': '',
    'port': '',
    'dbname': ''
}
db_config['username'] = os.environ.get("DBUSERNAME")
db_config['password'] = os.environ.get("DBPASS")
db_config['host'] = os.environ.get("DBHOST")
db_config['port'] = os.environ.get("DBPORT")
db_config['dbname'] = os.environ.get("DBNAME")

def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    if test_config is None:
        app.config.from_mapping(
            SECRET_KEY=os.environ.get("SECRET_KEY"),
            SQLALCHEMY_DATABASE_URI="postgresql://{}:{}@{}:{}/{}".format(
                db_config['username'],
                db_config['password'],
                db_config['host'],
                db_config['port'],
                db_config['dbname']
            ), #Ref(1): change approach in future
            SQLALCHEMY_TRACK_MODIFICATIONS=os.environ.get("SQLALCHEMY_TRACK_MODIFICATIONS")
        )   
    else:
        app.config.from_mapping(test_config)

    db.app = app
    db.init_app(app)
    cors.init_app(app)
    app.register_blueprint(stats)
    app.register_blueprint(utils)

    @app.errorhandler(HTTP_404_NOT_FOUND)
    def handle_404(e):
        return jsonify({'error': 'Not found'}), HTTP_404_NOT_FOUND

    @app.errorhandler(HTTP_500_INTERNAL_SERVER_ERROR)
    def handle_500(e):
        return jsonify({'error': 'Something went wrong, we are working on it'}), HTTP_500_INTERNAL_SERVER_ERROR

    return app


# Notes References
# Ref(1): Soution for hidden DB URL: https://stackoverflow.com/questions/35061914/how-to-change-database-url-for-a-heroku-application#:~:text=One%20way%20to%20edit%20the,that%20url%20of%20the%20database.
