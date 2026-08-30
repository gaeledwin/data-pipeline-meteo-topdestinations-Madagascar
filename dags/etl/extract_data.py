import requests
import json
from datetime import datetime
from airflow.decorators import task
from airflow.models import Variable


cities = [
    "Antananarivo",
    "Toamasina",
    "Antsiranana",
    "Mahajanga",
    "Fianarantsoa",
    "Toliara",
    "Morondava",
    "Antsirabe",
    "Sainte-Marie",
    "Nosy Be"
]

def extract_data(city_name, api_key):
    raw_data = []
    try:
        url = f'https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}'
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        data_column = {
            "city_id": data['id'],
            "city": data['name'],
            "latitude": data['coord']['lat'],
            "longitude": data['coord']['lon'],
            "temperature": data['main']['temp'],
            "feels_like": data['main']['feels_like'],
            "temperature_min": data['main']['temp_min'],
            "temperature_max": data['main']['temp_max'],
            "pressure_hpa": data['main']['pressure'],
            "humidity": data['main']['humidity'],
            "visibility": data.get('visibility'),
            "wind_speed": data['wind'].get('speed'),
            "wind_direction": data['wind'].get('deg'),
            "wind_gust": data['wind'].get('gust'),
            "cloudiness_percent": data['clouds']['all'],
            "weather_description": data['weather'][0]['description'],
            "observation_datetime": data['dt'],
            "sunrise": data['sys']['sunrise'],
            "sunset": data['sys']['sunset'],
            "timezone": data['timezone']
        }
        raw_data.append(data_column)
        return raw_data
    except requests.exceptions.RequestException as e:
        raise e

@task
def extract_data_by_city_name():
    api_key = Variable.get('WEATHER_API_KEY')
    all_weather_data = []
    for city in cities:
        data = extract_data(city, api_key)
        all_weather_data.extend(data)
    return all_weather_data

@task
def save_to_json_raw_data(data):
    file_path = f"/opt/airflow/data/raw_data/data_weather_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(data, json_outfile, indent=4, ensure_ascii=False)
    return file_path