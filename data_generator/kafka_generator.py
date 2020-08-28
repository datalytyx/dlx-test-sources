import json

from kafka import KafkaProducer, KafkaConsumer, TopicPartition


class Kafka:
    def __init__(self, args, logger):
        self.logger = logger
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

    def get_column_values(self):
        topic_partition = TopicPartition(topic=self.topic, partition=0)
        self.consumer.assign([topic_partition])
        self.consumer.seek_to_end(topic_partition)
        last_offset = self.consumer.position(topic_partition)
        if last_offset != 0:
            self.consumer.seek_to_beginning(topic_partition)
            for msg in self.consumer:
                if msg.offset == last_offset - 1:
                    break
            return msg.value
        return {}

    def set_column_values(self, columns, loop_counter, faker):
        if columns:
            generated_message = self.__get_template_message(loop_counter + columns['ID'], faker)
        else:
            generated_message = self.__get_template_message(loop_counter, faker)
        return generated_message

    def generate_query(self, row):
        return row

    def insert_and_commit(self, message):
        self.producer.send(self.topic, message)
        self.producer.flush()

    @staticmethod
    def __get_template_message(row_id, faker):
        """
        Returns a template message with random data
        :param row_id: Value to be used as 'ID' for message
        :return: Dict of random Data
        """
        return {
            'ID': row_id,
            'Name': {
                'FirstName': faker.first_name(),
                'LastName': faker.last_name(),
            },
            'Address': {
                'StreetAddress': faker.address(),
                'City': faker.city(),
                'PostalCode': int(faker.postalcode()),
                'Country': faker.country(),
            },
            'DateOfBirth': {
                'Day': int(faker.day_of_month()),
                'Month': faker.month_name(),
                'Year': int(faker.year()),
            },
            'Email': faker.ascii_safe_email(),
            'Job': faker.job(),
        }

