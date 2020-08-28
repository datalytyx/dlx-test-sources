import random
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
                                       password=args.password,
                                       use_pure=True)

    def __get_cursor(self):
        return self.connection.cursor(buffered=True)

    def close_connection(self):
        self.connection.close()

    def __run_query(self, sql_query):
        self.cursor.execute(sql_query)
        return self.cursor

    def get_column_values(self):
        column_values = {}
        sql_query = f"SELECT MAX(SalesOrderID) from {self.table}"
        column_values['MaxSalesOrderID'] = self.__run_query(sql_query).fetchone()[0]

        sql_query = f"SELECT RevisionNumber from {self.table} " \
                    f"WHERE RevisionNumber IS NOT NULL ORDER BY 1"
        column_values['RevisionNumbers'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(Status) from {self.table} " \
                    f"WHERE Status IS NOT NULL ORDER BY 1"
        column_values['Statuss'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(OnlineOrderFlag) from {self.table} " \
                    f"WHERE OnlineOrderFlag IS NOT NULL ORDER BY 1"
        column_values['OnlineOrderFlags'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(SalesOrderNumber) from {self.table} " \
                    f"WHERE SalesOrderNumber IS NOT NULL ORDER BY 1"
        column_values['SalesOrderNumbers'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(PurchaseOrderNumber) from {self.table} " \
                    f"WHERE PurchaseOrderNumber IS NOT NULL ORDER BY 1"
        column_values['PurchaseOrderNumbers'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(AccountNumber) from {self.table} " \
                    f"WHERE AccountNumber IS NOT NULL ORDER BY 1"
        column_values['AccountNumbers'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(CustomerID) from {self.table} " \
                    f"WHERE CustomerID IS NOT NULL ORDER BY 1"
        column_values['CustomerIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(ContactID) from {self.table} " \
                    f"WHERE ContactID IS NOT NULL ORDER BY 1"
        column_values['ContactIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(SalesPersonID) from {self.table} " \
                    f"WHERE SalesPersonID IS NOT NULL ORDER BY 1"
        column_values['SalesPersonIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(TerritoryID) from {self.table} " \
                    f"WHERE TerritoryID IS NOT NULL ORDER BY 1"
        column_values['TerritoryIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(BillToAddressID) from {self.table} " \
                    f"WHERE BillToAddressID IS NOT NULL ORDER BY 1"
        column_values['BillToAddressIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(ShipToAddressID) from {self.table} " \
                    f"WHERE ShipToAddressID IS NOT NULL ORDER BY 1"
        column_values['ShipToAddressIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(ShipMethodID) from {self.table} " \
                    f"WHERE ShipMethodID IS NOT NULL ORDER BY 1"
        column_values['ShipMethodIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(CreditCardID) from {self.table} " \
                    f"WHERE CreditCardID IS NOT NULL ORDER BY 1"
        column_values['CreditCardIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(CreditCardApprovalCode) from {self.table} " \
                    f"WHERE CreditCardApprovalCode IS NOT NULL ORDER BY 1"
        column_values['CreditCardApprovalCodes'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(CurrencyRateID) from {self.table} " \
                    f"WHERE CurrencyRateID IS NOT NULL ORDER BY 1"
        column_values['CurrencyRateIDs'] = self.__run_query(sql_query).fetchall()

        return column_values

    @staticmethod
    def set_column_values(columns, loop_counter, fake):
        random.seed(a=loop_counter, version=2)
        row = {
            'SalesOrderID': str(columns['MaxSalesOrderID'] + loop_counter),
            'RevisionNumber': str(random.choice(columns['RevisionNumbers'])[0]),
            'OrderDate': "now()",
            'DueDate': "now() + interval 1 day",
            'ShipDate': "now() + interval 2 day",
            'Status': str(random.choice(columns['Statuss'])[0]),
            'OnlineOrderFlag': random.choice(columns['OnlineOrderFlags'])[0],
            'SalesOrderNumber': str(random.choice(columns['SalesOrderNumbers'])[0]),
            'PurchaseOrderNumber': str(random.choice(columns['PurchaseOrderNumbers'])[0]),
            'AccountNumber': str(random.choice(columns['AccountNumbers'])[0]),
            'CustomerID': str(random.choice(columns['CustomerIDs'])[0]),
            'ContactID': str(random.choice(columns['ContactIDs'])[0]),
            'SalesPersonID': str(random.choice(columns['SalesPersonIDs'])[0]),
            'TerritoryID': str(random.choice(columns['TerritoryIDs'])[0]),
            'BillToAddressID': str(random.choice(columns['BillToAddressIDs'])[0]),
            'ShipToAddressID': str(random.choice(columns['ShipToAddressIDs'])[0]),
            'ShipMethodID': str(random.choice(columns['ShipMethodIDs'])[0]),
            'CreditCardID': str(random.choice(columns['CreditCardIDs'])[0]),
            'CreditCardApprovalCode': str(random.choice(columns['CreditCardApprovalCodes'])[0]),
            'CurrencyRateID': str(random.choice(columns['CurrencyRateIDs'])[0]),
            'SubTotal': random.random(),
            'Comment': fake.paragraph(nb_sentences=1, variable_nb_sentences=True, ext_word_list=None),
            'rowguid': 'xxx',
            'ModifiedDate': "now()"
        }
        row['TaxAmt'] = float(row['SubTotal']) * random.random() * 20
        row['Freight'] = row['SubTotal'] * random.random() * 30
        row['TotalDue'] = row['SubTotal'] + row['TaxAmt'] + row['Freight']
        return row

    def generate_query(self, row):
        sql_query = f"""
            INSERT INTO {self.table} 
            ( SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate, Status, OnlineOrderFlag, SalesOrderNumber,
            PurchaseOrderNumber, AccountNumber, CustomerID, ContactID, SalesPersonID, TerritoryID, BillToAddressID,
            ShipToAddressID, ShipMethodID, CreditCardID, CreditCardApprovalCode, CurrencyRateID, SubTotal,
            TaxAmt, Freight, TotalDue, Comment, rowguid,
            ModifiedDate )
            VALUES
            ( '{row['SalesOrderID']}', '{row['RevisionNumber']}', {row['OrderDate']}, {row['DueDate']},
            {row['ShipDate']}, '{row['Status']}', {row['OnlineOrderFlag']}, '{row['SalesOrderNumber']}',
            '{row['PurchaseOrderNumber']}', '{row['AccountNumber']}', '{row['CustomerID']}', '{row['ContactID']}',
            '{row['SalesPersonID']}', '{row['TerritoryID']}', '{row['BillToAddressID']}', '{row['ShipToAddressID']}',
            '{row['ShipMethodID']}', '{row['CreditCardID']}', '{row['CreditCardApprovalCode']}',
            '{row['CurrencyRateID']}', '{row['SubTotal']}', '{row['TaxAmt']}', '{row['Freight']}', '{row['TotalDue']}',
            '{row['Comment']}', '{row['rowguid']}', {row['ModifiedDate']} )
        """
        return sql_query

    def insert_and_commit(self, sql_query):
        self.__run_query(sql_query)
        self.connection.commit()

    def remove_ids(self, ids):
        sql_query = f"DELETE FROM \"{self.table}\" WHERE SalesOrderId > {ids}"
        self.__run_query(sql_query)
