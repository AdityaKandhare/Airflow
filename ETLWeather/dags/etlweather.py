from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

# Airflow connection IDs
HTTP_CONN_ID = 'open_meteo_api'
DB_CONN_ID = 'postgres_default'

# List of locations to iterate (simulate hover effect)
LOCATIONS = [
    {"city": "London", "lat": "51.5074", "lon": "-0.1278"},
    {"city": "New York", "lat": "40.7128", "lon": "-74.0060"},
    {"city": "Tokyo", "lat": "35.6895", "lon": "139.6917"}
]

# DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
    dag_id='multi_city_weather_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Extract and store weather data for multiple cities sequentially'
) as dag:

    @task()
    def fetch_weather_data(location):
        """Fetch weather info for one city."""
        hook = HttpHook(http_conn_id=HTTP_CONN_ID, method='GET')
        endpoint = f"/v1/forecast?latitude={location['lat']}&longitude={location['lon']}&current_weather=true"
        response = hook.run(endpoint)

        if response.status_code == 200:
            data = json.loads(response.text)
            data['city'] = location['city']  # attach city name
            return data
        else:
            raise Exception(f"Failed to get weather for {location['city']}")

    @task()
    def transform_data(raw):
        """Extract needed weather fields from raw data."""
        current = raw['current_weather']
        return {
            'city': raw['city'],
            'lat': raw['latitude'],
            'lon': raw['longitude'],
            'temp': current['temperature'],
            'wind_speed': current['windspeed'],
            'wind_dir': current['winddirection'],
            'weather_code': current['weathercode']
        }

    @task()
    def store_to_db(record):
        """Insert weather data into PostgreSQL."""
        hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS city_weather (
                city TEXT,
                lat FLOAT,
                lon FLOAT,
                temp FLOAT,
                wind_speed FLOAT,
                wind_dir FLOAT,
                weather_code INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cursor.execute("""
            INSERT INTO city_weather (city, lat, lon, temp, wind_speed, wind_dir, weather_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            record['city'], record['lat'], record['lon'], record['temp'],
            record['wind_speed'], record['wind_dir'], record['weather_code']
        ))

        conn.commit()
        cursor.close()

    # Dynamically create task chains per location
    for loc in LOCATIONS:
        raw = fetch_weather_data(loc)
        processed = transform_data(raw)
        store_to_db(processed)
