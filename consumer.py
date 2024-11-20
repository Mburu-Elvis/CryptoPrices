import asyncio
from confluent_kafka import Consumer, KafkaError, KafkaException
import psycopg2
import json
import os
from dotenv import load_dotenv
import socket


load_dotenv()

BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP_SERVERS')
GROUP_ID = os.environ.get('GROUP_ID')
TOPIC = os.environ.get('TOPIC')

DATABASE_NAME = os.environ.get('DATABASE_NAME')
DATABASE_USER = os.environ.get('DATABASE_USER')
DATABASE_PASSWORD = os.environ.get('PASSWORD')
DATABASE_HOST = os.environ.get('DATABASE_HOST')
PORT = os.environ.get('DATABASE_PORT')
TABLE_NAME = os.environ.get('TABLE_NAME')


# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest',
}

CONNECTION = f"postgres://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{PORT}/{DATABASE_NAME}"

with psycopg2.connect(CONNECTION) as conn:
    cursor = conn.cursor()

# Function to consume messages from Kafka synchronously
def kafka_poll_sync(consumer):
    msg = consumer.poll(1.0)  # Poll for messages with a timeout of 1 second
    if msg is None:
        return None
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event
            print(f"End of partition reached: {msg.topic()} [{msg.partition()}]")
        else:
            # Error handling
            raise KafkaException(msg.error())
        return None
    msg = msg.value().decode('utf-8')
    return json.loads(msg)

# Async function to process messages
async def process_message(message):
    print(f"Processing message: {message}")
    try:
        query = f"""
            INSERT INTO {TABLE_NAME} (time, base, currency, price)
            VALUES (%s, %s, %s, %s);
        """
        cursor.execute(query, (message['price_time'], message['base'], message['currency'], message['amount']))

    except (Exception, psycopg2.Error) as error:
        print(error)
    conn.commit()
    print(f"Finished processing: {message}")

# Async consumer loop
async def consume():
    consumer = Consumer(**KAFKA_CONFIG)
    consumer.subscribe([TOPIC])

    try:
        print("Consumer started. Listening for messages...")
        while True:
            # Poll for a message in a separate thread
            message = await asyncio.to_thread(kafka_poll_sync, consumer)
            if message is not None:
                # Process the message asynchronously
                await process_message(message)
            else:
                print("No message")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Ensure consumer is closed
        consumer.close()

# Entry point
if __name__ == "__main__":
    asyncio.run(consume())
