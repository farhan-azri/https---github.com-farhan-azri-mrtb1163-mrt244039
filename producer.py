import json
import pandas as pd
import openmeteo_requests
import requests_cache

from kafka import KafkaProducer
from retry_requests import retry
from datetime import datetime

today = datetime.today().strftime('%Y-%m-%d')

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)

openmeteo = openmeteo_requests.Client(session=retry_session)

url = "https://archive-api.open-meteo.com/v1/archive"

locations = [
    {
        "name": "Petaling",
        "latitude": 3.107260,
        "longitude": 101.606710
    },
    {
        "name": "Klang",
        "latitude": 3.043092,
        "longitude": 101.441392
    }
]

common_params = {
    "start_date": "2025-01-01",
    "end_date": today, #"2025-01-31",
    "hourly": [
        "rain",
        "precipitation",
        "temperature_2m",
        "wind_speed_10m",
        "wind_gusts_10m"
    ],
    "timezone": "Asia/Kuala_Lumpur"
}

all_hourly = []

for loc in locations:

    params = {
        **common_params,
        "latitude": loc["latitude"],
        "longitude": loc["longitude"]
    }

    responses = openmeteo.weather_api(url, params=params)

    response = responses[0]

    hourly = response.Hourly()

    hourly_df = pd.DataFrame({
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "rain": hourly.Variables(0).ValuesAsNumpy(),
        "precipitation": hourly.Variables(1).ValuesAsNumpy(),
        "temperature_2m": hourly.Variables(2).ValuesAsNumpy(),
        "wind_speed_10m": hourly.Variables(3).ValuesAsNumpy(),
        "wind_gust_10m": hourly.Variables(4).ValuesAsNumpy(),
        "location": loc["name"]
    })

    all_hourly.append(hourly_df)

combined_df = pd.concat(all_hourly)

combined_df.to_csv(
    "data/weather_data.csv",
    index=False
)

for _, row in combined_df.iterrows():

    record = row.to_dict()

    record["date"] = str(record["date"])

    producer.send(
        "weather-data",
        value=record
    )

producer.flush()

print(f"Sent {len(combined_df)} records to Kafka")