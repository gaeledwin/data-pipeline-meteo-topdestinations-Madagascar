import logging
from datetime import datetime
import json
import logging
from data_warehouse.data_util import get_conn_cursor, close_conn_cursor, create_table_destinations, create_table_weather_records, create_table_weather_types, insert_row_destination, insert_row_weather_records, insert_row_weather_type
from airflow.decorators import task

table = "weather_data"
logger = logging.getLogger(__name__)

table_destination = "destinations"
table_weather_types = "weather_types"
table_weather_records = "weather_records"

@task
def get_transformed_data(file_path):
    try:
        with open(file_path, "r", encoding="utf_8") as raw_data:
            data = json.load(raw_data)
        return data
    except FileNotFoundError as e:
        logger.error(f"Error load file :{file_path}")
        raise e
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in file: {file_path}")
        raise e
    
@task
def load_data(transformed_data):
    conn, cur = get_conn_cursor()
    try:
        create_table_destinations(table_destination, conn, cur)
        create_table_weather_types(table_weather_types, conn, cur)
        create_table_weather_records(table_weather_records, conn, cur)
        for row in transformed_data:
            insert_row_destination(table_destination, conn, cur, row)
            insert_row_weather_type(table_weather_types, conn, cur, row)
            
            request = f"SELECT weather_type_id FROM {table_weather_types} WHERE description=  %(weather_description)s"
            cur.execute(request, row)
            result = cur.fetchone()
            weather_type_id = result['weather_type_id']
            row['weather_type_id'] = weather_type_id
            insert_row_weather_records(table_weather_records, conn, cur, row)
    except Exception as e:
        conn.rollback()
        logger.error(f"Error load data: {e}")
        raise e
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)