from confluent_kafka import Consumer, KafkaError
import csv
import os
from datetime import datetime


#tettt
# Define the CSV file name
csv_file = "data.csv"


# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

# Create a Kafka consumer instance
consumer = Consumer(conf)

# Subscribe to a Kafka topic
topic = 'conn-events'  # Replace with your topic name
consumer.subscribe([topic])

# Consume messages
while True:
    msg = consumer.poll(1.0)
    
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('Reached end of partition')
        else:
            print(f'Error: {msg.error()}')
    else:
        print(f'received {msg.value().decode("utf-8")}')
        data_price = [(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg.value().decode("utf-8"))]
        if not os.path.exists(csv_file):
    # If it doesn't exist, create a new file and write header and data
            with open(csv_file, mode="w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["time", "price"])  # Header
                writer.writerows(data_price)
        else:
            # If it exists, open it in append mode and add data
            with open(csv_file, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerows(data_price)

