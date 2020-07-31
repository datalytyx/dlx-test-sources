import json

from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition
from kafka.errors import KafkaError


class Kafka:
    def __init__(self, args):
        self.producer = self.__init_producer(args)
        self.consumer = self.__init_consumer(args)
        self.topic = args.database

    @staticmethod
    def __init_producer(args):
        return KafkaProducer(bootstrap_servers=args.host,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    @staticmethod
    def __init_consumer(args):
        return KafkaConsumer(bootstrap_servers=args.host,
                             value_deserializer=lambda m: json.loads(m.decode('ascii')),
                             enable_auto_commit=False,
                             auto_offset_reset='earliest')

    def run_query(self, sql_query):
        pass

    def generate_column_values(self, loop_counter):
        return {}

    def generate_query(self, columns):
        return {}

    def insert_and_commit(self, sql_query):
        self.run_query(sql_query)
