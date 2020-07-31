import psycopg2


class PostgreSQL:
    def __init__(self, args, logger):
        self.logger = logger
        self.logger.warn("Currently only the table 'SalesOrderheader' present in schema 'Sales' in dataset "
                         "AdventureWorks is supported")
        self.connection = self.__init_connection(args)
        self.cursor = self.__get_cursor()
        self.schema = args.schema
        self.table = args.table
        if args.ids:
            self.remove_ids(args.ids)

    @staticmethod
    def __init_connection(args):
        return psycopg2.connect(host=args.host,
                                port=args.port,
                                database=args.database,
                                user=args.user,
                                password=args.password)

    def __get_cursor(self):
        return self.connection.cursor()

    def close_connection(self):
        self.connection.close()

    def __run_query(self, sql_query):
        self.cursor.execute(sql_query)

    def get_column_values(self):
        column_values = {}
        return column_values

    def set_column_values(self, columns, loop_counter):
        pass

    def generate_query(self, columns):
        return {}

    def insert_and_commit(self, sql_query):
        self.__run_query(sql_query)
        self.connection.commit()

    def remove_ids(self, ids):
        sql_query = f"DELETE FROM \"{self.schema}\".\"{self.table}\" WHERE SalesOrderId > {ids}"
        self.__run_query(sql_query)
