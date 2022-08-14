from src.constants.http_status_codes import HTTP_200_OK, HTTP_201_CREATED, HTTP_204_NO_CONTENT, HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND, HTTP_409_CONFLICT
from flask import Blueprint
from flask_cors import CORS, cross_origin
from flask_sqlalchemy import SQLAlchemy
from flask.json import jsonify
from src.utils.DashF1Tool import DashF1Tool

import json
import pandas as pd

stats = Blueprint("stats", __name__, url_prefix="/api/v1/stats")
df1Tool = DashF1Tool()
db = SQLAlchemy()
cors = CORS()

@stats.get("/<string:id>")
@cross_origin()
def getHomeStats(id):
    query = ""
    year = 2014
    if id == "driver-stats":
        query = f"""
                    SELECT rank, driver, team, points 
                    FROM public.udf_get_driver_stats({str(year)})
                """
    elif id == "constructor-stats":
        query = f"""
                    SELECT rank, team, points 
                    FROM public.udf_get_constructor_stats({str(year)})
                """
    df_drivers = pd.read_sql_query(query, con=db.engine)
    if df_drivers.empty:
        return jsonify({'message': 'Item not found'}), HTTP_404_NOT_FOUND

    obj = json.loads(df_drivers.to_json(orient="split"))
    request = {
        "columns": list(map(df1Tool.firstCharacterToUpper, obj['columns'])),
        "data": obj['data']
    }
    
    return request, HTTP_200_OK



