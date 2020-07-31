import mysql.connector


class MySQL:
    def __init__(self, args, logger):
        self.logger = logger
        self.logger.warn("Currently only the table 'SalesOrderHeader' in dataset AdventureWork is supported")
        self.connection = self.__init_connection(args)
        self.cursor = self.__get_cursor()
        self.schema = args.database
        self.table = args.table
        if args.ids:
            self.remove_ids(args.ids)

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

    def __run_query(self, sql_query):
        return self.cursor.execute(sql_query)

    def get_column_values(self):
        column_values = {}
        sql_query = f"SELECT MAX(SalesOrderID) from \"{self.table}\""
        column_values['MaxSalesOrderId'] = self.__run_query(sql_query).fetchone()[0]

        sql_query = f"SELECT DISTINCT(CustomerID) FROM \"{self.table}\" " \
                    f"WHERE CustomerID IS NOT NULL ORDER BY 1"
        column_values['CustomerIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(Status) FROM \"{self.table}\" " \
                    f"WHERE Status IS NOT NULL ORDER BY 1"
        column_values['Statuss'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(OnlineOrderFlag) FROM \"{self.table}\" " \
                    f"WHERE OnlineOrderFlag IS NOT NULL ORDER BY 1"
        column_values['OnlineOrderFlags'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(AccountNumber) FROM \"{self.table}\" " \
                    f"WHERE AccountNumber IS NOT NULL ORDER BY 1"
        column_values['AccountNumbers'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(TerritoryID) FROM \"{self.table}\" " \
                    f"WHERE TerritoryID IS NOT NULL ORDER BY 1"
        column_values['TerritoryIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(BillToAddressID) FROM \"{self.table}\" " \
                    f"WHERE BillToAddressID IS NOT NULL ORDER BY 1"
        column_values['BillToAddressIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(ShipToAddressID) FROM \"{self.table}\" " \
                    f"WHERE ShipToAddressID IS NOT NULL ORDER BY 1"
        column_values['ShipToAddressIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(ShipMethodID) FROM \"{self.table}\" " \
                    f"WHERE ShipMethodID IS NOT NULL ORDER BY 1"
        column_values['ShipMethodIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(CreditCardID) FROM \"{self.table}\" " \
                    f"WHERE CreditCardID IS NOT NULL ORDER BY 1"
        column_values['CreditCardIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(CurrencyRateID) FROM \"{self.table}\" " \
                    f"WHERE CurrencyRateID IS NOT NULL ORDER BY 1"
        column_values['CurrencyRateIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(ContactID) FROM \"{self.table}\" " \
                    f"WHERE ContactID IS NOT NULL ORDER BY 1"
        column_values['ContactIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(SalesPersonID) FROM \"{self.table}\" " \
                    f"WHERE SalesPersonID IS NOT NULL ORDER BY 1"
        column_values['SalesPersonIDs'] = self.__run_query(sql_query).fetchall()

        return column_values

    def generate_query(self, columns):
        return {}

    def insert_and_commit(self, sql_query):
        self.__run_query(sql_query)
        self.connection.commit()

    def remove_ids(self, ids):
        sql_query = f"DELETE FROM \"{self.table}\" WHERE SalesOrderId > {ids}"
        self.__run_query(sql_query)
