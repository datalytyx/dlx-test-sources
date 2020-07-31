import cx_Oracle
from data_generator import logger


class Oracle:
    def __init__(self, args):
        logger.warn("Currently only the table 'DIMCUSTOMER' in dataset "
                    "AdventureWorks (link: https://github.com/artofbi/Oracle-AdventureWorks) is supported")
        self.connection = self.__init_connection(args)
        self.cursor = self.__get_cursor()
        self.schema = args.schema
        self.table = args.table

    @staticmethod
    def __init_connection(args):
        return cx_Oracle.connect(args.username,
                                 args.password,
                                 cx_Oracle.makedsn(args.host, args.port, args.database),
                                 encoding='UTF-8')

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
