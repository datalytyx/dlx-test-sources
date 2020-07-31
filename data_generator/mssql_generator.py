import pyodbc


class MSSQL:
    def __init__(self, args, logger):
        self.logger = logger
        self.logger.warn("Currently only the table 'SalesOrderHeader' present in schema 'Sales' in dataset "
                         "AdventureWorks is supported")
        self.connection = self.__init_connection(args)
        self.cursor = self.__get_cursor()
        self.schema = args.schema
        self.table = args.table
        if args.time_shift:
            self.__set_time_shift()
        if args.ids:
            self.remove_ids(args.ids)
        self._set_identity_insert()

    @staticmethod
    def __init_connection(args):
        return pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s" %
                              (args.host, args.port, args.database, args.username, args.password))

    def __get_cursor(self):
        return self.connection.cursor()

    def close_connection(self):
        self.connection.close()

    def __run_query(self, sql_query):
        return self.cursor.execute(sql_query)

    def get_column_values(self):
        column_values = {}
        sql_query = f"SELECT MAX(SalesOrderID) from \"{self.schema}\".\"{self.table}\""
        column_values['MaxSalesOrderId'] = self.__run_query(sql_query).fetchone()[0]

        sql_query = f"SELECT DISTINCT(CustomerID) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE CustomerID IS NOT NULL ORDER BY 1"
        column_values['CustomerIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(Status) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE Status IS NOT NULL ORDER BY 1"
        column_values['Statuss'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(OnlineOrderFlag) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE OnlineOrderFlag IS NOT NULL ORDER BY 1"
        column_values['OnlineOrderFlags'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(AccountNumber) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE AccountNumber IS NOT NULL ORDER BY 1"
        column_values['AccountNumbers'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(TerritoryID) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE TerritoryID IS NOT NULL ORDER BY 1"
        column_values['TerritoryIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(BillToAddressID) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE BillToAddressID IS NOT NULL ORDER BY 1"
        column_values['BillToAddressIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(ShipToAddressID) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE ShipToAddressID IS NOT NULL ORDER BY 1"
        column_values['ShipToAddressIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(ShipMethodID) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE ShipMethodID IS NOT NULL ORDER BY 1"
        column_values['ShipMethodIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(CreditCardID) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE CreditCardID IS NOT NULL ORDER BY 1"
        column_values['CreditCardIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(CurrencyRateID) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE CurrencyRateID IS NOT NULL ORDER BY 1"
        column_values['CurrencyRateIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(SalesPersonID) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE SalesPersonID IS NOT NULL ORDER BY 1"
        column_values['SalesPersonIDs'] = self.__run_query(sql_query).fetchall()

        return column_values

    def generate_query(self, columns):
        return {}

    def insert_and_commit(self, sql_query):
        self.__run_query(sql_query)
        self.connection.commit()

    def _set_identity_insert(self):
        sql_query = f"SET IDENTITY_INSERT \"{self.schema}\".\"{self.table}\" ON"
        self.__run_query(sql_query)

    def __set_time_shift(self):
        sql_query = f"""
        declare @diff int
        set @diff = (select DATEDIFF(SECOND, max(ModifiedDate), GETDATE()) from {self.schema}.{self.table})
        update {self.schema}.{self.table} set ModifiedDate = DATEADD(second, @diff, ModifiedDate)
        """
        self.__run_query(sql_query)

    def remove_ids(self, ids):
        sql_query = f"DELETE FROM \"{self.schema}\".\"{self.table}\" WHERE SalesOrderId > {ids}"
        self.__run_query(sql_query)
