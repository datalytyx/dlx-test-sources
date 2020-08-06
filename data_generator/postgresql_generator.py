import uuid
import random
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

        sql_query = f"SELECT MAX(SalesOrderID) FROM \"{self.schema}\".\"{self.table}\""
        column_values['MaxSalesOrderId'] = self.__run_query(sql_query).fetchone()[0]

        sql_query = f"SELECT DISTINCT(RevisionNumber) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE RevisionNumber IS NOT NULL ORDER BY 1"
        column_values['RevisionNumbers'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(OrderDate) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE OrderDate IS NOT NULL ORDER BY 1"
        column_values['OrderDates'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(DueDate) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE DueDate IS NOT NULL ORDER BY 1"
        column_values['DueDates'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(ShipDate) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE ShipDate IS NOT NULL ORDER BY 1"
        column_values['ShipDates'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(Status) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE Status IS NOT NULL ORDER BY 1"
        column_values['Statuss'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(OnlineOrderFlag) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE OnlineOrderFlag IS NOT NULL ORDER BY 1"
        column_values['OnlineOrderFlags'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(PurchaseOrderNumber) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE PurchaseOrderNumber IS NOT NULL ORDER BY 1"
        column_values['PurchaseOrderNumbers'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(AccountNumber) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE AccountNumber IS NOT NULL ORDER BY 1"
        column_values['AccountNumbers'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(CustomerID) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE CustomerID IS NOT NULL ORDER BY 1"
        column_values['CustomerIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(SalesPersonID) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE SalesPersonID IS NOT NULL ORDER BY 1"
        column_values['SalesPersonIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(TerritoryID) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE TerritoryID IS NOT NULL ORDER BY 1"
        column_values['TerritoryIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(BillToAddressID) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE BillToAddressID IS NOT NULL ORDER BY 1"
        column_values['BillToAddressIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(ShipToAddressID) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE ShipToAddressID IS NOT NULL ORDER BY 1"
        column_values['ShipToAddressIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(ShipMethodID) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE ShipMethodID IS NOT NULL ORDER BY 1"
        column_values['ShipMethodIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(CreditCardID) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE CreditCardID IS NOT NULL ORDER BY 1"
        column_values['CreditCardIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(CreditCardApprovalCode) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE CreditCardApprovalCode IS NOT NULL ORDER BY 1"
        column_values['CreditCardApprovalCodes'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(CurrencyRateID) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE CurrencyRateID IS NOT NULL ORDER BY 1"
        column_values['CurrencyRateIDs'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(SubTotal) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE SubTotal IS NOT NULL ORDER BY 1"
        column_values['SubTotals'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(TaxAmt) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE TaxAmt IS NOT NULL ORDER BY 1"
        column_values['TaxAmts'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(Freight) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE Freight IS NOT NULL ORDER BY 1"
        column_values['Freights'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(TotalDue) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE TotalDue IS NOT NULL ORDER BY 1"
        column_values['TotalDues'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(Comment) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE Comment IS NOT NULL ORDER BY 1"
        column_values['Comments'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(RowGuid) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE RowGuid IS NOT NULL ORDER BY 1"
        column_values['RowGuids'] = self.__run_query(sql_query).fetchall()

        sql_query = f"SELECT DISTINCT(ModifiedDate) FROM \"{self.schema}\".\"{self.table}\" " \
                    f"WHERE ModifiedDate IS NOT NULL ORDER BY 1"
        column_values['ModifiedDates'] = self.__run_query(sql_query).fetchall()

        return column_values

    def set_column_values(self, columns, loop_counter, fake):
        random.seed(a=loop_counter, version=2)
        row = {
            'MaxSalesOrderId': str(columns['MaxSalesOrderId'] + loop_counter),
            'RevisionNumber': str(fake.random_digit_not_null()),
            'OrderDate': 'now()',
            'DueDates': 'now() + 1',
            'ShipDate': 'now() + 2',
            'Status': str(random.choice(columns['Statuss'])[0]),
            'OnlineOrderFlag': str(random.choice(columns['OnlineOrderFlags'])[0]),
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
            'SubTotal': random.random(),
            'Comment': fake.paragraph(nb_sentences=1, variable_nb_sentences=True, ext_word_list=None),
            'RowGuid': uuid.UUID(int=random.Random().getrandbits(128)),
            'ModifiedDate': 'now()'
        }
        row['TaxAmt'] = float(row['SubTotal']) * random.random() * 20
        row['Freight'] = row['SubTotal'] * random.random() * 30
        row['TotalDue'] = row['SubTotal'] + row['TaxAmt'] + row['Freight']
        return row

    # def generate_query(self, columns):
    def generate_query(self, row):
        sql_query = f"""
        INSERT INTO {self.schema}.{self.table}
        (MaxSalesOrderId, RevisionNumber, OrderDate, DueDates, ShipDate, Status, OnlineOrderFlag, PurchaseOrderNumber,
        AccountNumber, CustomerID, SalesPersonID, TerritoryID, BillToAddressID, ShipToAddressID, ShipMethodID,
        CreditCardID, CreditCardApprovalCode, CurrencyRateID, SubTotal, Comment, RowGuid, ModifiedDate)
        VALUES
        ('{row['MaxSalesOrderId']}', '{row['RevisionNumber']}', '{row['OrderDate']}', '{row['DueDates']}',
        '{row['ShipDate']}', '{row['Status']}', '{row['OnlineOrderFlag']}', '{row['PurchaseOrderNumber']}',
        '{row['AccountNumber']}', '{row['CustomerID']}', '{row['SalesPersonID']}', '{row['TerritoryID']}',
        '{row['BillToAddressID']}', '{row['ShipToAddressID']}', '{row['ShipMethodID']}', '{row['CreditCardID']}',
        '{row['CreditCardApprovalCode']}', '{row['CurrencyRateID']}', '{row['SubTotal']}', '{row['Comment']}',
        '{row['RowGuid']}', '{row['ModifiedDate']}')
        """
        return sql_query

    def insert_and_commit(self, sql_query):
        self.__run_query(sql_query)
        self.connection.commit()

    def remove_ids(self, ids):
        sql_query = f"DELETE FROM \"{self.schema}\".\"{self.table}\" WHERE SalesOrderId > {ids}"
        self.__run_query(sql_query)
