import os 
import fastf1
import pandas 

class ConnectorFastF1:
    def __init__(self, year:int=0) -> None:
        self.path = os.path.dirname(os.path.realpath(__file__))
        self.raceSession = year
        self.pathToCache = "\cache"
        
    def cache_load(self, cachePath: str):
        subdirname = os.path.basename(self.path)
        print(f"load cache to: {subdirname}{self.pathToCache}")
        return fastf1.Cache.enable_cache(cachePath)
        
    def get_race_schedule(self):
        self.cache_load(f"{self.path}{self.pathToCache}")
        race_schedule = fastf1.get_event_schedule(self.raceSession)
        return race_schedule, True


