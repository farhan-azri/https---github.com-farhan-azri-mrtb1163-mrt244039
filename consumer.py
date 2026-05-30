import json
import pandas as pd

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "weather-data",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x:
        json.loads(x.decode("utf-8"))
)

records = []

print("Waiting for messages...")

for msg in consumer:

    print(msg.value)

    records.append(msg.value)

    if len(records) >= 100:

        break

df = pd.DataFrame(records)

df.to_csv(
    "data/kafka_output.csv",
    index=False
)

print("Data saved")