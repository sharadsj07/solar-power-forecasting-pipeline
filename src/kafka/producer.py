import json
import time

import pandas as pd
from kafka import KafkaProducer

producer = None

try:
    # Create Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Read CSV File
    df = pd.read_csv("data/processed/solar_featured_data_final.csv")

    print("=" * 60)
    print("Kafka Producer Started")
    print(f"Total records found: {len(df)}")
    print("=" * 60)

    # Send each row to Kafka
    for index, row in df.iterrows():

        # Convert row to dictionary
        message = row.to_dict()

        # Send message to Kafka topic
        producer.send("solar-data", value=message)

        print(f"Sent Record {index + 1}: {message}")

        # Simulate real-time streaming
        time.sleep(1)

    # Send remaining buffered messages
    producer.flush()

    print("\nStreaming completed successfully.")

except FileNotFoundError:
    print("Error: solar_featured_data.csv not found.")

except KeyboardInterrupt:
    print("\nStreaming stopped by user.")

except Exception as e:
    print(f"Unexpected Error: {e}")

finally:
    if producer is not None:
        producer.close()

    print("Producer connection closed.")
