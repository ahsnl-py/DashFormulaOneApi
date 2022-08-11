from src.constants.http_status_codes import HTTP_200_OK, HTTP_201_CREATED, HTTP_204_NO_CONTENT, HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND, HTTP_409_CONFLICT
from flask import Blueprint
from flask_cors import CORS, cross_origin
from flask_sqlalchemy import SQLAlchemy
from flask.json import jsonify

import json
import pandas as pd

stats = Blueprint("stats", __name__, url_prefix="/api/v1/stats")
db = SQLAlchemy()
cors = CORS()

@stats.get("/<string:id>")
@cross_origin()
def getClientRequestQuery(id):
    query = ""
    if id == "driver-stats":
        query = f"""
                    SELECT 	constructors_name, driver_fullname, sum(point) as Points
                            ,row_number() over(order by sum(point) desc)
                    FROM public.vw_race_results 
                    where year = '2019'
                    group by constructors_name, driver_fullname
                    order by sum(point) desc
                    limit 10
                """
    elif id == "constructor-stats":
        query = f"""
                    SELECT 	constructors_name, sum(point) as Points
                            ,row_number() over(order by sum(point) desc)
                    FROM public.vw_race_results 
                    where year = '2019'
                    group by constructors_name
                    order by sum(point) desc
                    limit 10
                """
    df_drivers = pd.read_sql_query(query, con=db.engine)
    obj = json.loads(df_drivers.to_json(orient="table"))
    request = {
        "schema": obj['schema']['fields'],
        "data": obj['data']
    }
    
    return request, HTTP_200_OK

