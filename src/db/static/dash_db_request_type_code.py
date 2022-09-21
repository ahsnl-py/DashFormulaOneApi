
def static_request_type_code():
    
    query = """
                INSERT INTO public.request_type(
                    request_type_id, request_type_code, request_type_desc, request_type_group, request_job_name)
                VALUES (%s, %s, %s, %s, %s);
            """
    values = [
        ('1', 'gp-schedule', 'Load GP Race Events Release Dates', 'events-info', 'df1_ufl_gp_schedule',),
        ('2', 'gp-weather', 'Load GP Race Events Release Weather Temperature', 'events-info', 'df1_ufl_gp_weather',),
        ('3', 'gp-race-results', 'Load GP Weekends Race Sessions Results', 'race-info', 'df1_ufl_race_results',)
    ]
    return query, values

