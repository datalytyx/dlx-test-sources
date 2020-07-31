import mysql.connector

from data_generator import logger


class MySQL:
    def __init__(self, args):
        logger.warn("Currently only the table 'SalesOrderHeader' in dataset AdventureWork is supported")
        self.connection = self.__init_connection(args)
        self.cursor = self.__get_cursor()
        self.schema = args.database
        self.table = args.table

    @staticmethod
    def __init_connection(args):
        return mysql.connector.connect(host=args.host,
                                       port=args.port,
                                       database=args.database,
                                       user=args.username,
                                       password=args.password)

    def __get_cursor(self):
        return self.connection.cursor()

    def close_connection(self):
        self.connection.close()

    def run_query(self, sql_query):
        self.cursor.excute(sql_query)

    def generate_column_values(self, loop_counter):
        return {}

    def generate_query(self, columns):
        return {}

    def insert_and_commit(self, sql_query):
        self.run_query(sql_query)
        self.connection.commit()
