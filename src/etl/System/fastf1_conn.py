import json
import os 
import fastf1
import pandas 

class ConnectorFastF1:
    def __init__(self) -> None:
        self.path = os.path.dirname(os.path.realpath(__file__))
        self.pathToCache = "\cache"
        
    def cache_load(self):
        subdirname = os.path.basename(self.path)
        print(f"load cache to: {subdirname}{self.pathToCache}")
        return fastf1.Cache.enable_cache(f"{self.path}{self.pathToCache}")
        
    def get_race_schedule(self, year:str):
        self.cache_load()
        race_schedule = fastf1.get_event_schedule(year)
        return race_schedule, True


    def get_race_results(self, gp:str, race_year:int, race_type:str, schema:list):
        self.cache_load()
        session = fastf1.get_session(race_year, gp, race_type)
        session.load(laps=True, telemetry=True, weather=False, messages=False, livedata=None)
        table_res = session.results
        table_res = table_res.loc[:, schema]
        table_res['Q1'] = table_res['Q1'].astype(str).replace({'NaT': None})
        table_res['Q2'] = table_res['Q2'].astype(str).replace({'NaT': None})
        table_res['Q3'] = table_res['Q3'].astype(str).replace({'NaT': None})
        table_res['Time'] = table_res['Time'].astype(str).replace({'NaT': None})
        items = {}
        for i in table_res.index:
            items[i] = list(table_res.loc[i])    
        return json.dumps({
            'columns' : list(table_res.columns),
            'data': items
        })

    def map_race_type(self, race_type):
        # https://theoehrly.github.io/Fast-F1/events.html#session-identifiers
        race_type_code = {
            "Practice 1": "P1",
            "Practice 2": "P2",
            "Practice 3": "P3",
            "Sprint Qualifying": "SQ",
            "Qualifying": "Q",
            "Race": "R"
        }
        return race_type_code[race_type]

        


