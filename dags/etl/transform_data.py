from datetime import datetime, timezone, timedelta
import logging
import json
from airflow.decorators import task

logger = logging.getLogger(__name__)

def kelvin_to_celsius(value):
    if value is None:
        return None
    else:
        return(value - 273.15)

def m_to_km(value):
    if value is None:
        return None
    else:
        return (value / 1000)

def to_local_timestamp(value, timezone_offset):
    tz = timezone(timedelta(seconds=timezone_offset))
    result = datetime.fromtimestamp(value, tz=tz)
    return result

@task
def get_raw_data(file_path):
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
def transform_data(raw_data_list):
    transformed = []
    for raw_data in raw_data_list:
        
        timezone_offset = raw_data["timezone"]

        raw_data['temperature'] = kelvin_to_celsius(raw_data['temperature'])
        raw_data['feels_like'] = kelvin_to_celsius(raw_data['feels_like'])
        raw_data['temperature_min'] = kelvin_to_celsius(raw_data['temperature_min'])
        raw_data['temperature_max'] = kelvin_to_celsius(raw_data['temperature_max'])
        raw_data['visibility'] = m_to_km(raw_data['visibility'])
        raw_data['observation_datetime'] = to_local_timestamp(raw_data['observation_datetime'], timezone_offset).isoformat()
        raw_data['sunrise'] = to_local_timestamp(raw_data['sunrise'], timezone_offset).isoformat()
        raw_data['sunset'] = to_local_timestamp(raw_data['sunset'], timezone_offset).isoformat()
        raw_data['record_id'] = f"{raw_data['city_id']}_{raw_data['observation_datetime']}"
        transformed.append(raw_data)
    
    return transformed

@task
def save_to_json_transformed_data(data):
    file_path = f"/opt/airflow/data/transformed_data/data_weather_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(data, json_outfile, indent=4, ensure_ascii=False)
    return file_path
    
    
