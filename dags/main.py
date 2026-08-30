from airflow import DAG
import pendulum
from datetime import timedelta, datetime
from etl.extract_data import extract_data_by_city_name, save_to_json_raw_data
from etl.transform_data import get_raw_data, transform_data, save_to_json_transformed_data
from etl.load_data import get_transformed_data, load_data

local_tz = pendulum.timezone('Indian/Antananarivo')

default_args = {
    "owner": "edwingael",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "gaeldewin@gmail.com",
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 8, 1, tzinfo=local_tz)
}

with DAG(
    dag_id="produce_data_weather.json",
    default_args=default_args,
    description="produce fichier json for meteo madagascar data",
    schedule="0 6,9,12,15,18,21 * * *",
    catchup=False
) as dag:
    
    data = extract_data_by_city_name()
    save_to_json_raw_data_task = save_to_json_raw_data(data)
    raw_data = get_raw_data(save_to_json_raw_data_task)
    transformed_data = transform_data(raw_data)
    save_to_json_transformed_data_task = save_to_json_transformed_data(transformed_data)
    real_data = get_transformed_data(save_to_json_transformed_data_task)
    load_data_task = load_data(real_data)
    
    data >> save_to_json_raw_data_task >> raw_data >> transformed_data >> save_to_json_transformed_data_task >> real_data >> load_data_task
    