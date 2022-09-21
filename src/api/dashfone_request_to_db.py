from src.constants.http_status_codes import HTTP_200_OK, HTTP_201_CREATED, HTTP_204_NO_CONTENT, HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND, HTTP_409_CONFLICT
from flask import Blueprint
from flask_cors import CORS, cross_origin
from flask_sqlalchemy import SQLAlchemy
from flask.json import jsonify
from src.utils.DashF1Tool import DashF1Tool

import json
import pandas as pd

utils = Blueprint("utils", __name__, url_prefix="/api/v1/utils")
df1Tool = DashF1Tool()
db = SQLAlchemy()
cors = CORS()


@utils.get("/validate/<string:race_date>/<string:code>")
@cross_origin()
def ValidateRaceDateByCode(race_date:str, code:str):
    query = f"""
        SELECT _isexists FROM public.udf_validate_race_date_by_code('{race_date}', '{code}')
    """
    df_validate_date = pd.read_sql_query(query, con=db.engine)
    if df_validate_date.empty:
        return jsonify({'message': 'Item not found'}), HTTP_404_NOT_FOUND
    parsed = json.loads(df_validate_date.to_json(orient="records"))
    return json.dumps(parsed, indent=4), HTTP_201_CREATED

