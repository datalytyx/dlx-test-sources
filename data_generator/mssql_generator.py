import random

import pyodbc


class MSSQL:
    def __init__(self, args, logger):
        self.logger = logger
        self.logger.warn("Currently only the tables 'SalesOrderHeader' and 'Customer' present in "
                         "schema 'Sales' in dataset AdventureWorks are supported")
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

    def get_salesorderheader(self, column_values):
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

    def get_customer(self, column_values):
        sql_query = f"SELECT MAX(CustomerID) from \"{self.schema}\".\"{self.table}\""
        column_values['MaxCustomerId'] = self.__run_query(sql_query).fetchone()[0]

        sql_query = f"SELECT DISTINCT(PersonID) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE PersonID IS NOT NULL ORDER BY 1"
        column_values['PersonIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(StoreID) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE StoreID IS NOT NULL ORDER BY 1"
        column_values['StoreIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(TerritoryID) FROM \"{self.schema}\".{self.table} " \
                    f"WHERE TerritoryID IS NOT NULL ORDER BY 1"
        column_values['TerritoryIDs'] = self.__run_query(sql_query).fetchall()

    def get_column_values(self):
        column_values = {}
        if self.table.lower() == 'salesorderheader':
            self.get_salesorderheader(column_values)
        elif self.table.lower() == 'customer':
            self.get_customer(column_values)

        return column_values

    @staticmethod
    def set_salesorderheader(columns, loop_counter, fake):
        random.seed(a=loop_counter, version=2)
        row = {
            'SalesOrderID': str(columns['MaxSalesOrderId'] + loop_counter),
            'RevisionNumber': str(fake.random_digit_not_null()),
            'OrderDate': "getdate()",
            'DueDate': "getdate() + 1",
            'ShipDate': "getdate() + 2",
            'Status': str(random.choice(columns['Statuss'])[0]),
            'OnlineOrderFlag': str(random.choice(columns['OnlineOrderFlags'])[0]),
            'SalesOrderNumber': str(fake.isbn10(separator="-")),
            'PurchaseOrderNumber': str(fake.isbn13(separator="-")),
            'AccountNumber': str(random.choice(columns['AccountNumbers'])[0]),
            'CustomerID': str(random.choice(columns['CustomerIDs'])[0]),
            'SalesPersonID': str(random.choice(columns['SalesPersonIDs'])[0]),
            'TerritoryID': str(random.choice(columns['TerritoryIDs'])[0]),
            'BillToAddressID': str(random.choice(columns['BillToAddressIDs'])[0]),
            'ShipToAddressID': str(random.choice(columns['ShipToAddressIDs'])[0]),
            'ShipMethodID': str(random.choice(columns['ShipMethodIDs'])[0]),
            'CreditCardID': str(random.choice(columns['CreditCardIDs'])[0]),
            'CreditCardApprovalCode': fake.credit_card_security_code(card_type=None),
            'CurrencyRateID': str(random.choice(columns['CurrencyRateIDs'])[0]),
            'SubTotal': random.random() * 300,
            'Comment': fake.paragraph(nb_sentences=1, variable_nb_sentences=True, ext_word_list=None),
            'ModifiedDate': "getdate()",
        }
        row['TaxAmt'] = row['SubTotal'] * random.random() * 20
        row['Freight'] = row['SubTotal'] * random.random() * 30
        row['TotalDue'] = row['SubTotal'] + row['TaxAmt'] + row['Freight']

        return row

    @staticmethod
    def set_customer(columns, loop_counter, fake):
        random.seed(a=loop_counter, version=2)
        row = {
            'CustomerID': str(columns['MaxCustomerId'] + loop_counter),
            'PersonID': str(random.choice(columns['PersonIDs'])[0]),
            'StoreID': str(random.choice(columns['StoreIDs'])[0]),
            'TerritoryID': str(random.choice(columns['TerritoryIDs'])[0]),
            'RowGuid': "newid()",
            'ModifiedDate': "getdate()",
        }

        return row

    def set_column_values(self, columns, loop_counter, fake):
        row = {}
        if self.table.lower() == 'salesorderheader':
            row = MSSQL.set_salesorderheader(columns, loop_counter, fake)
        elif self.table.lower() == 'customer':
            row = MSSQL.set_customer(columns, loop_counter, fake)

        return row

    def query_customer(self, row):
        sql_query = f"""
        INSERT INTO \"{self.schema}\".\"{self.table}\" 
        (CustomerID, PersonID, StoreID, TerritoryID, rowguid, ModifiedDate) 
        VALUES 
        ({row['CustomerID']}, {row['PersonID']}, {row['StoreID']}, {row['TerritoryID']}, 
        ({row['RowGuid']}), ({row['ModifiedDate']}))
        """
        return sql_query

    def query_salesorderheader(self, row):
        sql_query = f"""
        INSERT INTO \"{self.schema}\".\"{self.table}\" 
        (SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate, Status, PurchaseOrderNumber, AccountNumber, 
        CustomerID, SalesPersonID, TerritoryID, BillToAddressID, ShipToAddressID, ShipMethodID, CreditCardID, 
        CreditCardApprovalCode, CurrencyRateID, SubTotal, TaxAmt, Freight, Comment, ModifiedDate) 
        VALUES 
        ('{row['SalesOrderID']}', '{row['RevisionNumber']}', ({row['OrderDate']}), ({row['DueDate']}), 
        ({row['ShipDate']}), '{row['Status']}', '{row['PurchaseOrderNumber']}', '{row['AccountNumber']}', 
        '{row['CustomerID']}', '{row['SalesPersonID']}', '{row['TerritoryID']}', '{row['BillToAddressID']}', 
        '{row['ShipToAddressID']}', '{row['ShipMethodID']}', '{row['CreditCardID']}', '{row['CreditCardApprovalCode']}', 
        '{row['CurrencyRateID']}', '{str(row['SubTotal'])}', '{str(row['TaxAmt'])}', '{str(row['Freight'])}', 
        '{row['Comment']}', ({row['ModifiedDate']}))
        """

        return sql_query

    def generate_query(self, row):
        sql_query = ""
        if self.table.lower() == 'salesorderheader':
            sql_query = self.query_salesorderheader(row)
        elif self.table.lower() == 'customer':
            sql_query = self.query_customer(row)

        return sql_query

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
