import argparse
import json
from typing import Dict, Generator

from faker import Faker
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition
from kafka.errors import KafkaError

parser = argparse.ArgumentParser(description='Kafka Incremental Data Generator')
parser.add_argument('--host', dest='host', help='Bootstrap Servers list', required=True)
parser.add_argument('--topic', dest='topic', help='Name of the Topic', required=True)
parser.add_argument('--type', dest='type', type=str, default='incremental', choices=['incremental', 'create'],
                    help='Type of insertion to be made(New row or Incremental)')
parser.add_argument('--rows', dest='rows', help='Rows to be inserted in Topic', type=int, default=0)
parser.add_argument('--sleep', dest='sleep', type=int, default=0, help='Seconds pause between row/message insertion')
parser.add_argument('--counter', dest='counter', type=int, default=100,
                    help='Counter after which to display number of inserted rows')
args = parser.parse_args()

fake = Faker()


def get_template_message(row_id: int):
    """
    Returns a template message with random data
    :param row_id: Value to be used as 'ID' for message
    :return: Dict of random Data
    """
    return {
        'ID': row_id,
        'Name': {
            'FirstName': fake.first_name(),
            'LastName': fake.last_name(),
        },
        'Address': {
            'StreetAddress': fake.address(),
            'City': fake.city(),
            'PostalCode': int(fake.postalcode()),
            'Country': fake.country(),
        },
        'DateOfBirth': {
            'Day': int(fake.day_of_month()),
            'Month': fake.month_name(),
            'Year': int(fake.year()),
        },
        'Email': fake.ascii_safe_email(),
        'Job': fake.job(),
    }


def get_last_message() -> Dict:
    """
    Returns the last message of the queue in the specified topic
    :return: Dict of message if any message else None
    """
    consumer = KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')),
                             enable_auto_commit=False,
                             auto_offset_reset='earliest')
    topic_partition = TopicPartition(topic=args.topic, partition=0)
    consumer.assign([topic_partition])
    consumer.seek_to_end(topic_partition)
    last_offset = consumer.position(topic_partition)
    if last_offset != 0:
        consumer.seek_to_beginning(topic_partition)
        for msg in consumer:
            if msg.offset == last_offset - 1:
                break
        return msg.value
    return {}


def generate_messages() -> Generator:
    """
    Generator for returning message with randomly generated data
    :return: Dict Generator
    """
    last_message = get_last_message()
    rows_inserted = 0
    while True and (args.rows == 0 or rows_inserted < args.rows):
        rows_inserted += 1
        yield generate_random_data(rows_inserted, last_message)


def generate_random_data(rows_inserted: int, message: dict) -> Dict:
    """
    Returns randomly generated data
    :param rows_inserted: Number of rows inserted into queue
    :param message: Message format
    :return: Returns a dictionary object with randomly generated data
    """
    if message:
        generated_message = get_template_message(rows_inserted + message['ID'])
    else:
        generated_message = get_template_message(rows_inserted)
    return generated_message


def send_message() -> None:
    """
    Adds a message with randomly generated into the kafka queue
    :return: None
    """
    try:
        producer = KafkaProducer(bootstrap_servers=args.host,
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        loop_counter = 0
        for i in generate_messages():
            producer.send(args.topic, value=i)
            producer.flush()
            loop_counter += 1
            if loop_counter % args.counter == 0:
                print('Messages inserted since start:', loop_counter)
    except KafkaError as e:
        print(f'Exception raised during execution:\n{e}')


send_message()
