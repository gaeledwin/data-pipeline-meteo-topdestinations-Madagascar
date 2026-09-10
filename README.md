# Pipeline ETL Météo — Madagascar

Pipeline de données automatisé qui collecte, transforme et stocke les relevés météo en temps réel des 10 principales destinations touristiques de Madagascar, avec un dashboard Power BI pour visualiser les tendances.

![CI/CD](https://github.com/gaeledwin/data-pipeline-meteo-topdestinations-Madagascar/actions/workflows/CI-CD-DATA_METEO.yaml/badge.svg)
![Python](https://img.shields.io/badge/python-3.x-blue)
![Airflow](https://img.shields.io/badge/orchestration-Apache%20Airflow-red)
![Docker](https://img.shields.io/badge/containerized-Docker-blue)

## Sommaire

- [Architecture](#architecture)
- [Stack technique](#stack-technique)
- [Installation](#installation)
- [Utilisation](#utilisation)
- [Modèle de données](#modèle-de-données)
- [Dashboard Power BI](#dashboard-power-bi)
- [CI/CD](#cicd)

## Architecture

Le pipeline s'exécute 6 fois par jour (6h, 9h, 12h, 15h, 18h, 21h, heure de Madagascar) via un DAG Apache Airflow, en 3 étapes :

1. **Extract** — récupération des données météo en temps réel via l'API OpenWeatherMap pour les 10 destinations touristiques suivies
2. **Transform** — conversion des unités (Kelvin → Celsius, mètres → kilomètres), passage des timestamps au fuseau horaire local, structuration des données
3. **Load** — insertion dans une base PostgreSQL, organisée en 3 tables (destinations, types de temps, relevés météo)

Le pipeline est entièrement conteneurisé avec Docker, et testé automatiquement à chaque push via GitHub Actions.

```mermaid
graph LR
    A[OpenWeatherMap API] --> B[Extract]
    B --> C[Transform]
    C --> D[Load]
    D --> E[(PostgreSQL)]
    E --> F[Power BI Dashboard]
```

## Stack technique

- **Orchestration** : Apache Airflow
- **Langage** : Python (pandas pour la manipulation de données)
- **Base de données** : PostgreSQL
- **Conteneurisation** : Docker / Docker Compose
- **CI/CD** : GitHub Actions
- **Source de données** : API OpenWeatherMap
- **Visualisation** : Power BI

## Installation

### Prérequis
- Docker et Docker Compose installés
- Une clé API [OpenWeatherMap](https://openweathermap.org/api) (gratuite)

### Étapes

1. Clone le repo :
```bash
git clone https://github.com/gaeledwin/data-pipeline-meteo-topdestinations-Madagascar.git
cd TON_REPO
```

2. Crée un fichier `.env` à la racine du projet avec les variables suivantes :
```
DOCKERHUB_NAMESPACE=
DOCKERHUB_REPOSITORY=

POSTGRES_CONN_USERNAME=
POSTGRES_CONN_PASSWORD=
POSTGRES_CONN_HOST=
POSTGRES_CONN_PORT=

METADATA_DATABASE_NAME=
METADATA_DATABASE_USERNAME=
METADATA_DATABASE_PASSWORD=

CELERY_BACKEND_NAME=
CELERY_BACKEND_USERNAME=
CELERY_BACKEND_PASSWORD=

ETL_DATABASE_NAME=
ETL_DATABASE_USERNAME=
ETL_DATABASE_PASSWORD=

AIRFLOW_UID=
AIRFLOW_WWW_USER_USERNAME=
AIRFLOW_WWW_USER_PASSWORD=
FERNET_KEY=

WEATHER_API_KEY=
```

3. Lance les conteneurs :
```bash
docker compose up -d
```

4. Accède à l'interface Airflow sur [http://localhost:8080](http://localhost:8080) (identifiants définis dans `AIRFLOW_WWW_USER_USERNAME` / `AIRFLOW_WWW_USER_PASSWORD`).

## Modèle de données

La base PostgreSQL est organisée en 3 tables reliées entre elles :

### `destinations`
| Colonne | Type | Description |
|---|---|---|
| city_id | INT (PK) | Identifiant de la ville |
| city_name | VARCHAR | Nom de la destination |
| latitude | FLOAT | Latitude |
| longitude | FLOAT | Longitude |
| timezone | INT | Décalage horaire (secondes) |

### `weather_types`
| Colonne | Type | Description |
|---|---|---|
| weather_type_id | SERIAL (PK) | Identifiant du type de temps |
| description | VARCHAR | Description (ex: "clear sky", "light rain") |

### `weather_records`
| Colonne | Type | Description |
|---|---|---|
| record_id | VARCHAR (PK) | Identifiant unique du relevé |
| city_id | INT (FK) | Référence vers `destinations` |
| weather_type_id | INT (FK) | Référence vers `weather_types` |
| temperature, feels_like, temperature_min, temperature_max | FLOAT | Températures (°C) |
| pressure_hpa | INT | Pression atmosphérique |
| humidity | INT | Humidité (%) |
| visibility | FLOAT | Visibilité (km) |
| wind_speed, wind_direction, wind_gust | FLOAT/INT | Données de vent |
| cloudiness_percent | INT | Nébulosité (%) |
| observation_datetime, sunrise, sunset | TIMESTAMP | Horodatages |

## Dashboard Power BI

Le dashboard permet d'explorer les tendances météo des 10 destinations touristiques suivies.

### Vue d'ensemble
![Vue d'ensemble du dashboard](images/dashboard-overview.jpg)

### Types de temps et températures par ville
![Types de temps](images/dashboard-types-temps.jpg)

### Vent, visibilité et nébulosité
![Vent et nébulosité](images/dashboard-vent.jpg)

## CI/CD

Le projet utilise **GitHub Actions** pour automatiser la construction et les tests à chaque push sur `main`, `master` ou une branche `features/*`, ainsi qu'à chaque pull request.

Le workflow comporte 2 jobs :

1. **build-and-push-image** — construit l'image Docker du projet et la pousse sur DockerHub (taguée `latest` et avec le SHA du commit)
2. **quality-test-and-e2e-tests** — lance l'environnement complet via Docker Compose et exécute un test end-to-end du DAG Airflow (`airflow dags test produce_data_weather.json`) pour vérifier que le pipeline s'exécute sans erreur

Toutes les variables sensibles (identifiants base de données, clé API, etc.) sont stockées en tant que **secrets GitHub**, jamais en clair dans le code.

## Auteur

Réalisé par Gaël Edwin — projet portfolio en data engineering / data analytics.