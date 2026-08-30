from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
import logging

table_dest = "destinations"
table_weather_types = "weather_types"
table_weather_records = "weather_records"

logger = logging.getLogger(__name__)

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_weather_etl", database='etl_db')
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    return conn, cur

def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()
    
def create_table_weather_records(table_weather_records, conn, cur):
    request = f"""
        CREATE TABLE IF NOT EXISTS {table_weather_records} (
            "record_id" VARCHAR(60) PRIMARY KEY,
            "city_id" INT NOT NULL,
            "weather_type_id" INT NOT NULL,
            "temperature" FLOAT NOT NULL,
            "feels_like" FLOAT NOT NULL,
            "temperature_min" FLOAT NOT NULL,
            "temperature_max" FLOAT NOT NULL,
            "pressure_hpa" INT NOT NULL,
            "humidity" INT NOT NULL,
            "visibility" FLOAT NOT NULL,
            "wind_speed"  FLOAT NOT NULL,
            "wind_direction" INT,
            "wind_gust" FLOAT,
            "cloudiness_percent" INT NOT NULL,
            "observation_datetime" TIMESTAMP NOT NULL,
            "sunrise" TIMESTAMP NOT NULL, 
            "sunset" TIMESTAMP NOT NULL,
            
            CONSTRAINT fk_weather_city
                FOREIGN KEY (city_id)
                REFERENCES destinations(city_id),
            CONSTRAINT fk_weather_type
            FOREIGN KEY (weather_type_id)
                REFERENCES weather_types(weather_type_id)
        );
    """
    cur.execute(request)
    conn.commit()
    
def create_table_destinations(table_dest, conn, cur):
    request = f"""
            CREATE TABLE IF NOT EXISTS {table_dest} (
                "city_id" INT PRIMARY KEY,
                "city_name" VARCHAR(20) NOT NULL,
                "latitude" FLOAT NOT NULL,
                "longitude" FLOAT NOT NULL,
                "timezone" INT NOT NULL
            );
        """
    cur.execute(request)
    conn.commit()
    

def create_table_weather_types(table_weather_types, conn, cur):
    request = f"""
            CREATE TABLE IF NOT EXISTS {table_weather_types} (
                "weather_type_id" SERIAL PRIMARY KEY,
                "description" VARCHAR(20) NOT NULL UNIQUE
            );
        """
    cur.execute(request)
    conn.commit()
    
def insert_row_destination(table, conn, cur, row):

    try:
        request = f"""
                INSERT INTO {table} ("city_id", "city_name", "latitude", "longitude", "timezone")
                VALUES (%(city_id)s, %(city)s, %(latitude)s, %(longitude)s, %(timezone)s)
                ON CONFLICT ("city_id") DO NOTHING;
            """
        cur.execute(request, row)
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting data in table {table}: {e}")
        raise e
    
def insert_row_weather_records(table, conn, cur, row):

    try:
        request = f"""
                INSERT INTO {table} ("record_id","city_id", "weather_type_id", "temperature", "feels_like", "temperature_min", "temperature_max",
                "pressure_hpa", "humidity", "visibility", "wind_speed", "wind_direction", "wind_gust", "cloudiness_percent",
                "observation_datetime", "sunrise", "sunset")
                VALUES (%(record_id)s, %(city_id)s, %(weather_type_id)s, %(temperature)s, %(feels_like)s, %(temperature_min)s, %(temperature_max)s,
                %(pressure_hpa)s, %(humidity)s, %(visibility)s, %(wind_speed)s, %(wind_direction)s, %(wind_gust)s, %(cloudiness_percent)s,
                %(observation_datetime)s, %(sunrise)s, %(sunset)s)
                ON CONFLICT ("record_id") DO NOTHING;
            """
        cur.execute(request, row)
        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting data in table {table}: {e}")
        raise e
    
def insert_row_weather_type(table, conn, cur, row):

    try:
        request = f"""
                INSERT INTO {table} ("description")
                VALUES (%(weather_description)s)
                ON CONFLICT ("description") DO NOTHING;
            """
        cur.execute(request, row)
        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting data in table {table}: {e}")
        raise e
    