import json

from kafka import KafkaConsumer

consumer = None

try:
    # Create Kafka Consumer
    consumer = KafkaConsumer(
        "solar-data",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    print("=" * 60)
    print("Kafka Consumer Started")
    print("Waiting for messages...")
    print("=" * 60)

    # Read messages continuously
    for message in consumer:
        print(message.value)

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")

except Exception as e:
    print(f"Unexpected Error: {e}")

finally:
    if consumer is not None:
        consumer.close()

    print("Consumer connection closed.")