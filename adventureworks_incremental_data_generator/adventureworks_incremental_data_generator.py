import mysql.connector
from mysql.connector import Error
from faker import Faker
import getopt
import sys
import time
import argparse
import random
import pyodbc

parser = argparse.ArgumentParser(description='AdventureWorks Incremental Data Generator')
parser.add_argument('--host', metavar='host', help='mysql ip address')
parser.add_argument('--port', metavar='N', help='mysql port')
parser.add_argument('--database', metavar='N', help='mysql database')
parser.add_argument('--username', metavar='N', help='mysql username')
parser.add_argument('--password', metavar='N', help='mysql password')
parser.add_argument('--type', metavar='N', help='mysql|mssql')
parser.add_argument('--sleep', type=float, metavar='N', default=0, help='sleep seconds between row inserts')
parser.add_argument('--schema', type=str, metavar='N', help='mssql schema name')
parser.add_argument('--removeidsabove', type=str, metavar='N', help='Delete all rows with a SalesOrderId over this value')
parser.add_argument('--timeshifttonow', action="store_true")

args = parser.parse_args()
SCHEMA = ''

try:
    if args.type == 'mysql':
        connection = mysql.connector.connect(host=args.host, port=args.port, database=args.database, user=args.username,
                                             password=args.password)
        SCHEMA = f'{args.database}.'
        cursor = connection.cursor()
    elif args.type == 'mssql':
        connection = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s" %
                                    (args.host, args.port, args.database, args.username, args.password))

        SCHEMA = f'{args.database}.{args.schema}.'
        cursor = connection.cursor()
    else:
        print("--type not set to either mysql or mssql")
        exit()
    if args.removeidsabove:
        query = f"delete from {SCHEMA}salesorderheader where SalesOrderId > " + args.removeidsabove
        print("Truncating salesorderheader using: " + query)
        cursor.execute(query)
        connection.commit()
    if args.timeshifttonow:
        sqlcmd = """
        declare @diff int
        set @diff = (select DATEDIFF(SECOND, max(ModifiedDate), GETDATE()) from Sales.SalesOrderHeader)
        update Sales.SalesOrderHeader set ModifiedDate = DATEADD(second, @diff, ModifiedDate)
        """
        print("Timeshifting using: " + sqlcmd)
        cursor.execute(sqlcmd)
        connection.commit()
        cursor.execute(f"select max(ModifiedDate) from Sales.SalesOrderHeader")
        maxdate = cursor.fetchall()
        print("Max date is now: "+str(maxdate))

    if args.type == 'mssql':
        query = f"SET IDENTITY_INSERT {SCHEMA}salesorderheader ON"
        print("Enabling with: " + query)
        cursor.execute(query)

    query = f"select max(SalesOrderID) from {SCHEMA}salesorderheader"
    cursor.execute(query)
    MaxSalesOrderID = cursor.fetchone()[0]
    # print("max SaleOrderID=",MaxSalesOrderID)

    cursor.execute(f"select distinct(CustomerID) from {SCHEMA}salesorderheader where CustomerID is not null order by 1")
    CustomerIDs = cursor.fetchall()
    cursor.execute(f"select distinct(Status) from {SCHEMA}salesorderheader where Status is not null order by 1")
    Statuss = cursor.fetchall()
    cursor.execute(
        f"select distinct(OnlineOrderFlag) from {SCHEMA}salesorderheader where OnlineOrderFlag is not null order by 1")
    OnlineOrderFlags = cursor.fetchall()
    cursor.execute(
        f"select distinct(AccountNumber) from {SCHEMA}salesorderheader where AccountNumber is not null order by 1")
    AccountNumbers = cursor.fetchall()
    cursor.execute(
        f"select distinct(TerritoryID) from {SCHEMA}salesorderheader where TerritoryID is not null order by 1")
    TerritoryIDs = cursor.fetchall()
    cursor.execute(
        f"select distinct(BillToAddressID) from {SCHEMA}salesorderheader where BillToAddressID is not null order by 1")
    BillToAddressIDs = cursor.fetchall()
    cursor.execute(
        f"select distinct(ShipToAddressID) from {SCHEMA}salesorderheader where ShipToAddressID is not null order by 1")
    ShipToAddressIDs = cursor.fetchall()
    cursor.execute(
        f"select distinct(ShipMethodID) from {SCHEMA}salesorderheader where ShipMethodID is not null order by 1")
    ShipMethodIDs = cursor.fetchall()
    cursor.execute(
        f"select distinct(CreditCardID) from {SCHEMA}salesorderheader where CreditCardID is not null order by 1")
    CreditCardIDs = cursor.fetchall()
    cursor.execute(
        f"select distinct(CurrencyRateID) from {SCHEMA}salesorderheader where CurrencyRateID is not null order by 1")
    CurrencyRateIDs = cursor.fetchall()
    if args.type == 'mysql':
        cursor.execute(
            f"select distinct(ContactID) from {SCHEMA}salesorderheader where ContactID is not null order by 1")
        ContactIDs = cursor.fetchall()
    cursor.execute(
        f"select distinct(SalesPersonID) from {SCHEMA}salesorderheader where SalesPersonID is not null order by 1")
    SalesPersonIDs = cursor.fetchall()
#    cursor.execute("select distinct(rowguid) from {SCHEMA}salesorderheader order by 1")
#    rowguids=cursor.fetchall()

except Error as e:
    print("Error connecting to mysql: ", e)
# finally:
#    if (connection.is_connected()):
#        connection.close()
#        cursor.close()
#        print("Connections closed")

loop_counter = 0
while True:
    loop_counter = loop_counter + 1
    if (loop_counter % 100) == 0:
        print("Total rows inserted since start: ", loop_counter)
    fake = Faker()
    fake.seed_instance(loop_counter)  # by using the loop counter as a seed, the data is both random, but repeatable
    random.seed(a=loop_counter, version=2)
    SalesOrderID = str(MaxSalesOrderID + loop_counter)
    RevisionNumber = str(fake.random_digit_not_null())
    OrderDate = "now()" if args.type == 'mysql' else "getdate()"
    DueDate = str(fake.date_time_this_year(after_now=False, tzinfo=None)) if args.type == 'mysql' else "getdate() + 1"
    ShipDate = str(fake.date_time_this_year(after_now=False, tzinfo=None)) if args.type == 'mysql' else "getdate() + 2"
    Status = str(random.choice(Statuss)[0])
    OnlineOrderFlag = str(random.choice(OnlineOrderFlags)[0])
    SalesOrderNumber = str(fake.isbn10(separator="-"))
    PurchaseOrderNumber = str(fake.isbn13(separator="-"))
    AccountNumber = str(random.choice(AccountNumbers)[0])
    CustomerID = str(random.choice(CustomerIDs)[0])
    if args.type == 'mysql':
        ContactID = str(random.choice(ContactIDs)[0])
    SalesPersonID = str(random.choice(SalesPersonIDs)[0])
    TerritoryID = str(random.choice(TerritoryIDs)[0])
    BillToAddressID = str(random.choice(BillToAddressIDs)[0])
    ShipToAddressID = str(random.choice(ShipToAddressIDs)[0])
    ShipMethodID = str(random.choice(ShipMethodIDs)[0])
    CreditCardID = str(random.choice(CreditCardIDs)[0])
    CreditCardApprovalCode = fake.credit_card_security_code(card_type=None)
    CurrencyRateID = str(random.choice(CurrencyRateIDs)[0])
    SubTotal = random.random() * 300
    TaxAmt = SubTotal * random.random() * 20
    Freight = SubTotal * random.random() * 30
    TotalDue = SubTotal + TaxAmt + Freight
    Comment = fake.paragraph(nb_sentences=1, variable_nb_sentences=True, ext_word_list=None)
    # rowguid=random.choice(rowguids)[0]
    ModifiedDate = "now()" if args.type == 'mysql' else "getdate()"
    if args.type == 'mysql':
        query = f"INSERT INTO {SCHEMA}salesorderheader (SalesOrderID,RevisionNumber,OrderDate,DueDate,ShipDate,Status,OnlineOrderFlag,SalesOrderNumber,PurchaseOrderNumber,AccountNumber,CustomerID,ContactID,SalesPersonID,TerritoryID,BillToAddressID,ShipToAddressID,ShipMethodID,CreditCardID,CreditCardApprovalCode,CurrencyRateID,SubTotal,TaxAmt,Freight,TotalDue,Comment,rowguid,ModifiedDate) VALUES ("
    else:
        query = f"INSERT INTO {SCHEMA}salesorderheader (SalesOrderID,RevisionNumber,OrderDate,DueDate,ShipDate,Status,PurchaseOrderNumber,AccountNumber,CustomerID,SalesPersonID,TerritoryID,BillToAddressID,ShipToAddressID,ShipMethodID,CreditCardID,CreditCardApprovalCode,CurrencyRateID,SubTotal,TaxAmt,Freight,Comment,ModifiedDate) VALUES ("
    query += "'" + SalesOrderID + "',"
    query += "'" + RevisionNumber + "',"
    query += "(" + OrderDate + "),"
    query += "'" + DueDate + "'," if args.type == 'mysql' else "(" + DueDate + "),"
    query += "'" + ShipDate + "'," if args.type == 'mysql' else "(" + ShipDate + "),"
    query += "'" + Status + "',"
    if args.type == 'mysql':
        query += "" + OnlineOrderFlag + ","
        query += "'" + SalesOrderNumber + "',"
    query += "'" + PurchaseOrderNumber + "',"
    query += "'" + AccountNumber + "',"
    query += "'" + CustomerID + "',"
    if args.type == 'mysql':
        query += "'" + ContactID + "',"
    query += "'" + SalesPersonID + "',"
    query += "'" + TerritoryID + "',"
    query += "'" + BillToAddressID + "',"
    query += "'" + ShipToAddressID + "',"
    query += "'" + ShipMethodID + "',"
    query += "'" + CreditCardID + "',"
    query += "'" + CreditCardApprovalCode + "',"
    query += "'" + CurrencyRateID + "',"
    query += "'" + str(SubTotal) + "',"
    query += "'" + str(TaxAmt) + "',"
    query += "'" + str(Freight) + "',"
    if args.type == 'mysql':
        query += "'" + str(TotalDue) + "',"
    query += "'" + Comment + "',"
    if args.type == 'mysql':
        query += "'" + "xxx" + "',"
    query += "(" + ModifiedDate + ")"
    query += ");"

    # print(query)
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    time.sleep(args.sleep)
