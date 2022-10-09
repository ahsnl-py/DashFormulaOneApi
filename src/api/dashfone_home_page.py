from src.constants.http_status_codes import HTTP_200_OK, HTTP_201_CREATED, HTTP_204_NO_CONTENT, HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND, HTTP_409_CONFLICT
from flask import Blueprint
from flask_cors import CORS, cross_origin
from flask_sqlalchemy import SQLAlchemy
from flask.json import jsonify
from src.utils.DashF1Tool import DashF1Tool

import datetime
import json
import pandas as pd

stats = Blueprint("stats", __name__, url_prefix="/api/v1/stats")
df1Tool = DashF1Tool()
db = SQLAlchemy()
cors = CORS()


@stats.get("/<string:code>/<string:race_date>")
@cross_origin()
def getHomeOverviewStats(race_date:str, code:str):
    query = f"""
        SELECT json_data FROM public.udf_get_gp_race_res('{race_date}', '{code}')
    """
    df_race_res = pd.read_sql_query(query, con=db.engine)

    query_race_name = f"""
        SELECT race_country_event, race_name_event 
        from public.udf_get_race_details_by_date('{race_date}')
    """
    df_race_name_details = pd.read_sql_query(query_race_name, con=db.engine)

    if df_race_res.empty or df_race_name_details.empty:
        return jsonify({'message': 'Item not found'}), HTTP_404_NOT_FOUND
    race_data = df_race_res.loc[0]['json_data']
    request = []
    for k in race_data['data'].keys():
          arr_1 = race_data['data'][k]
          arr_2 = race_data['columns']
          request.append(dict(zip(arr_2, arr_1)))
    # select top three finisher only
    sort_request = sorted(request, key=lambda d: d['Points'], reverse=True) 
    request = {
        'front_runner_data': sort_request[:3],
        'race_event_info': json.loads(df_race_name_details.to_json(orient="records"))
    }
    return json.dumps(request), HTTP_200_OK
    

@stats.get("/<string:id>/<int:ryear>")
@cross_origin()
def getHomeStats(ryear:int, id:str):
    query = ""
    if id == "driver-stats":
        query = f"""
                    SELECT standing_pos as Rank, 
                            full_name   as Driver,
                            team        as Team,
                            points      as Points,
                            team_color  as TeamColor
                    FROM public.udf_get_drivers_standings_by_year('{str(ryear)}')
                """
    elif id == "constructor-stats":
        query = f"""
                    SELECT standing_pos     as Rank, 
                            team_name       as Driver,
                            drivers         as Team,
                            points          as Point
                    FROM public.udf_get_constructors_standings_by_year('{str(ryear)}')
                """ 

    elif id == "driver-stats-charts":
        query = f"""
                    select  race_event_date AS event_date, 
                            race_event_points AS event_point
                    from public.udf_get_driver_standings_by_year_json('{str(ryear)}')
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

@stats.get("/event/<string:schedule_type>/<int:year>")
@cross_origin()
def getHomeSchedule(schedule_type:str, year:int):
    print(schedule_type)
    query:str
    if schedule_type == 'race-schedule':
        query = f"""    
                select 
                    race_country,
                    race_location,
                    race_event_name_official,                
                    race_session_one,
                    race_session_one_date,
                    race_session_two,
                    race_session_two_date,
                    race_session_three,
                    race_session_three_date,
                    race_session_four,
                    race_session_four_date,
                    race_session_five,
                    race_session_five_date
                from vw_race_session_schedule
                where date_part('year', race_date) = '{str(year)}'
        """
    elif schedule_type == 'race-date-list':
        query = f"""
                    SELECT json_agg(date(race_date)) as labels
                    FROM public.vw_race_dates_schedule
                    WHERE date_part('Year', race_date) = '{str(year)}'
                """
    df_json = pd.read_sql_query(query, con=db.engine)
    if df_json.empty:
        return jsonify({'message': 'Item not found'}), HTTP_404_NOT_FOUND
    
    parse:object
    if schedule_type == 'race-schedule':
        parse = df1Tool.parseGPSchedule(df_json.to_json(orient="split"))
    else:
        parse = json.loads(df_json.to_json(orient="records"))[0]        
    return json.dumps(parse), HTTP_200_OK



