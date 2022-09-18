import pandas as pd
import psycopg2
import os
import datetime as dt
from config.config import set_config

class ConnectorDB:

    def __init__(self, db_config_file:str, db_config_sec:str) -> None:
        self.params = set_config(config_file_name = db_config_file, config_sec_name = db_config_sec)
        self.conn = psycopg2.connect(**self.params)
        self.filePath = '{0}\{1}'.format(os.path.dirname(os.path.realpath(__file__)), 'cache\csv')  
        self.dbObject = "" 

    def create_request(self, 
        request_code:str, 
        race_date:str,
        request_status_id:int, 
        request_start_date:dt.datetime="",
        request_end_date:dt.datetime=""):
        
        # create new id for each request operations and 
        # return the id as an output
        try:            
            with self.conn:
                with self.conn.cursor() as curs:
                    operation_proc = f"""
                            CALL public.create_request(
                                '{request_code}'::varchar(20),
                                '{race_date}'::date, 
                                {request_status_id}::int2, 
                                '{request_start_date}'::timestamp, 
                                {request_end_date}::timestamp);
                            """                    
                    # data = (race_date, request_type_id, request_status_id, request_type_message, request_start_date, request_end_date)
                    curs.execute(operation_proc)
                    if curs.rowcount == 0:
                        with self.conn.cursor() as curs_bu:
                            query = f"""
                                        SELECT MAX(request_id) as request_id 
                                        FROM public.request
                                    """    
                            curs_bu.execute(query)
                            if curs_bu.rowcount > 0:
                                print("RequestId:"+ str(curs_bu.fetchone()[0]))
                                return curs_bu.fetchone()
                            return 0
                    return curs.fetchone()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)                
        finally:                
            self.conn.commit()
        return              

    def close_request(self, 
        request_id:int,
        request_status_id:int,
        request_end_date:str,
        request_type_code:str=""):
        # update request when job complete
        try:            
            with self.conn:
                with self.conn.cursor() as curs:
                    operation_proc = f"""
                            CALL public.close_request(
                                {request_id}::int, 
                                {request_status_id}::smallint,
                                '{request_type_code}'::varchar(30), 
                                '{request_end_date}'::timestamp);
                            """                    
                    curs.execute(operation_proc)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)                
        finally:                
            self.conn.commit()
        return

    def load_to_csv(self, df, file_name) -> bool:
        exportPath = '{0}\{1}'.format(self.filePath, file_name)
        if not os.path.exists(exportPath):
            df.to_csv(exportPath, sep=',', encoding='utf-8', index=False, header='true')    
        return True if len(pd.read_csv(exportPath)) == len(df) else False

    def load_from_csv(self, file_name:str, db_obj:str):
        try:
            operation = ""
            with self.conn:
                with self.conn.cursor() as curs:
                    operation = f"""
                        COPY public.{db_obj} ({','.join(self.get_db_obj_cols(db_obj, curs))})
                        FROM '{self.filePath}\{file_name}'
                        DELIMITER ','
                        CSV HEADER
                    """
                with self.conn.cursor() as curs:
                    curs.execute(operation)
                    log_row_count = f"Details: Number of rows affected by statement: {curs.rowcount} rows affected."
                    if curs.rowcount > 0:
                        return True, log_row_count
                    return False, log_row_count

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)                
        finally:                
            self.conn.commit()
        return 

    def get_db_obj_cols(self, db_obj, curs) -> list:
        query = f"""
                SELECT * 
                FROM public.{db_obj} 
                LIMIT 1
            """
        curs.execute(query)
        return [cols[0] for cols in curs.description if cols[0] != "race_id"]
    
    def get_race_info_by_date(self, race_date:str):
        try:
            select = f"""
                SELECT 
                    _id, 
                    race_type, 
                    race_time, 
                    race_name
                FROM public.udf_get_race_info_by_date('{race_date}')
            """
            with self.conn:
                with self.conn.cursor() as curs:
                    curs.execute(select)
                    if curs.rowcount > 0:
                        # return curs.fetchall()
                        df = pd.DataFrame(curs.fetchall(), 
                                    columns=[
                                        "_id", 
                                        "race_type", 
                                        "race_time", 
                                        "race_name"
                                    ])
                        df.set_index('race_time', drop=False)
                        return df
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)                
        
    def dump_race_results(self, gp_res:list):
        sql = f"""
                INSERT INTO public.fact_race_gp_results_raw(
                request_id, race_date, race_type, json_data)
                VALUES (%s, %s, %s, %s);
            """
        self.execute_many(sql, gp_res)
        return True 
                
    def execute_many(self, statement:str, res_list:list, rollback_on_error:bool=True):
        try:
            with self.conn:
                with self.conn.cursor() as cur:
                    cur.executemany(statement, res_list)
                    if not rollback_on_error:
                        self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            if rollback_on_error:
                self.conn.rollback()
            raise
        finally:
            if self.conn is not None and rollback_on_error:
                self.conn.commit()

    def execute_multiple(self, conn, statements, rollback_on_error=True):
        """
        Execute multiple SQL statements and returns the cursor from the last executed statement.

        :param conn: The connection to the database
        :type conn: Database connection

        :param statements: The statements to be executed
        :type statements: A list of strings

        :param: rollback_on_error: Flag to indicate action to be taken on an exception
        :type rollback_on_error: bool

        :returns cursor from the last statement executed
        :rtype cursor
        """

        try:
            cursor = conn.cursor()
            for statement in statements:
                cursor.execute(statement)
                if not rollback_on_error:
                    conn.commit() # commit on each statement
        except Exception as e:
            if rollback_on_error:
                conn.rollback()
            raise
        else:
            if rollback_on_error:
                conn.commit() # then commit only after all statements have completed successfully