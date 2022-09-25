from distutils.log import debug
import os
import calendar
import pycountry
import json
from datetime import datetime, timedelta


class DashF1Tool:
    
    def firstCharacterToUpper(self, characters: str) -> list:
        return characters[0].upper() + characters[1:]


    def parseGPSchedule(self, json_data:str):
        schedule_data = json.loads(json_data)
        gp_schedule = []
        for idx in schedule_data['index']:
            gp_schedule_time = []
            event_date_duration = ""
            arr_1 = schedule_data['columns']
            arr_2 = schedule_data['data'][idx]
            schedule_details = dict(zip(arr_1, arr_2))
            for k, val in schedule_details.items():

                if k in ['race_session_one_date', 'race_session_two_date', 'race_session_three_date', 'race_session_four_date', 'race_session_five_date']:
                    if val is not None:
                        timestamp = int(str(val)[:len(str(val))-3]) # ugly way to split last digits number :( 
                        dt_object = datetime.fromtimestamp(timestamp) # convert form timestamp to datetime
                        t = datetime.strptime(str(dt_object), '%Y-%m-%d %H:%M:%S') 
                        y = datetime.strptime(str(dt_object + timedelta(hours = 1)), '%Y-%m-%d %H:%M:%S')
                        calendar_day = calendar.day_name[dt_object.weekday()]
                        time_duration = f"{t.strftime('%H:%M')} - {y.strftime('%H:%M')}"
                        event_date_duration = f"{str(dt_object.date() - timedelta(days=3)).replace('-', '.')} - {str(dt_object.date()).replace('-', '.')}"
                        gp_schedule_time.append([
                            self.get_schedule_type_event_code(schedule_details[k.replace("_date", "")]),
                            calendar_day[:3],
                            time_duration
                        ])
            
            country = schedule_details['race_country']
            if country == 'Great Britain':
                country = 'United Kingdom'
            elif country == 'Abu Dhabi':
                country = 'United Arab Emirates'

            gp_schedule.append({
                'id': idx,
                'gpCountry': country,
                'gpCountryCode': pycountry.countries.lookup(country).alpha_2,
                'gpDates': event_date_duration,
                'gpRaceEventOffical': schedule_details['race_event_name_official'],
                'gpScheduleTime': gp_schedule_time,
                'gpLocation': schedule_details['race_location']
            })

        return gp_schedule
      
    def get_schedule_type_event_code(self, event_type:str):
        race_type_code = {
                "Practice 1": "FP 1",
                "Practice 2": "FP 2",
                "Practice 3": "FP 3",
                "Sprint": "SPRI",
                "Qualifying": "QUAL",
                "Race": "RACE"
        }
        return race_type_code[event_type]
        