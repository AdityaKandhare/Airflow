from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

# Constants
LAT = '51.5074'
LON = '-0.1278'
HTTP_CONN_ID = 'open_meteo_api'
DB_CONN_ID = 'postgres_default'

# Default DAG arguments
args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

# DAG definition
with DAG(
    dag_id='daily_weather_etl',
    default_args=args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL pipeline to fetch, process, and store weather data for London'
) as dag:

    @task()
    def fetch_weather():
        """Fetch current weather information from Open-Meteo API."""
        hook = HttpHook(http_conn_id=HTTP_CONN_ID, method='GET')
        api_path = f'/v1/forecast?latitude={LAT}&longitude={LON}&current_weather=true'
        response = hook.run(api_path)

        if response.status_code == 200:
            return json.loads(response.text)
        else:
            raise RuntimeError(f"API request failed with status code {response.status_code}")

    @task()
    def process_weather(data):
        """Process JSON weather data into a structured format."""
        current = data.get("current_weather", {})
        return {
            'lat': LAT,
            'lon': LON,
            'temp': current.get('temperature'),
            'wind_speed': current.get('windspeed'),
            'wind_dir': current.get('winddirection'),
            'weather_code': current.get('weathercode')
        }

    @task()
    def store_weather(record):
        """Store the processed weather data into PostgreSQL."""
        db = PostgresHook(postgres_conn_id=DB_CONN_ID)
        conn = db.get_conn()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather_info (
                lat FLOAT,
                lon FLOAT,
                temp FLOAT,
                wind_speed FLOAT,
                wind_dir FLOAT,
                weather_code INTEGER,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cur.execute("""
            INSERT INTO weather_info (lat, lon, temp, wind_speed, wind_dir, weather_code)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            record['lat'],
            record['lon'],
            record['temp'],
            record['wind_speed'],
            record['wind_dir'],
            record['weather_code']
        ))

        conn.commit()
        cur.close()

    # Define task dependencies
    raw_data = fetch_weather()
    structured_data = process_weather(raw_data)
    store_weather(structured_data)
