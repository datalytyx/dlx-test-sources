import mysql.connector
from mysql.connector import Error
from faker import Faker
import getopt
import sys
import time
import argparse
import random
import pymssql

parser = argparse.ArgumentParser(description='AdventureWorks Incremental Data Generator')
parser.add_argument('--host', metavar='host', help='mysql ip address')
parser.add_argument('--port', metavar='N',    help='mysql port')
parser.add_argument('--database', metavar='N',help='mysql database')
parser.add_argument('--username', metavar='N',help='mysql username')
parser.add_argument('--password', metavar='N',help='mysql password')
parser.add_argument('--type', metavar='N',help='mysql|mssql')
parser.add_argument('--sleep', type=float, metavar='N', default=0, help='sleep seconds between row inserts')

args = parser.parse_args()


try:
    if (args.type == 'mysql'): 
      connection = mysql.connector.connect(host=args.host,port=args.port,database=args.database,user=args.username,password=args.password)
      cursor = connection.cursor()
    elif (args.type == 'mssql'):
      #connection = pymssql.connect(server=args.host,port=args.port,database=args.database,user=args.username,password=args.password)
      connection = pymssql.connect(args.host,args.username,args.password,args.database)
      cursor = connection.cursor()
    else:
      print("--type not set to either mysql or mssql")
      exit()
    query="select max(SalesOrderID) from salesorderheader"
    cursor.execute(query)
    MaxSalesOrderID=cursor.fetchone()[0]
    #print("max SaleOrderID=",MaxSalesOrderID)

    print("Getting CustomerIDs")
    cursor.execute("select distinct(CustomerID) from salesorderheader where CustomerID is not null order by 1")
    CustomerIDs=cursor.fetchall()
    print("Getting Status")
    cursor.execute("select distinct(Status) from salesorderheader where Status is not null order by 1")
    Statuss=cursor.fetchall()
    print("Getting OnlineOrderFlag")
    cursor.execute("select distinct(OnlineOrderFlag) from salesorderheader where OnlineOrderFlag is not null order by 1")
    OnlineOrderFlags=cursor.fetchall()
    cursor.execute("select distinct(AccountNumber) from salesorderheader where AccountNumber is not null order by 1")
    AccountNumbers=cursor.fetchall()
    cursor.execute("select distinct(TerritoryID) from salesorderheader where TerritoryID is not null order by 1")
    TerritoryIDs=cursor.fetchall()
    cursor.execute("select distinct(BillToAddressID) from salesorderheader where BillToAddressID is not null order by 1")
    BillToAddressIDs=cursor.fetchall()
    cursor.execute("select distinct(ShipToAddressID) from salesorderheader where ShipToAddressID is not null order by 1")
    ShipToAddressIDs=cursor.fetchall()
    cursor.execute("select distinct(ShipMethodID) from salesorderheader where ShipMethodID is not null order by 1")
    ShipMethodIDs=cursor.fetchall()
    cursor.execute("select distinct(CreditCardID) from salesorderheader where CreditCardID is not null order by 1")
    CreditCardIDs=cursor.fetchall()
    cursor.execute("select distinct(CurrencyRateID) from salesorderheader where CurrencyRateID is not null order by 1")
    CurrencyRateIDs=cursor.fetchall()
    cursor.execute("select distinct(ContactID) from salesorderheader where ContactID is not null order by 1")
    ContactIDs=cursor.fetchall()
    cursor.execute("select distinct(SalesPersonID) from salesorderheader where SalesPersonID is not null order by 1")
    SalesPersonIDs=cursor.fetchall()
#    cursor.execute("select distinct(rowguid) from salesorderheader order by 1")
#    rowguids=cursor.fetchall()


except Error as e:
    print("Error connecting to mysql: ",e)
#finally:
#    if (connection.is_connected()):
#        connection.close()
#        cursor.close()
#        print("Connections closed")

loop_counter=0
while True:
    loop_counter = loop_counter+1
    if (loop_counter % 100) == 0:
        print("Total rows inserted since start: ",loop_counter)
    fake = Faker()
    fake.seed_instance(loop_counter)  # by using the loop counter as a seed, the data is both random, but repeatable
    random.seed(a=loop_counter, version=2)
    SalesOrderID=str(MaxSalesOrderID+loop_counter)
    RevisionNumber=str(fake.random_digit_not_null())
    OrderDate="now()"
    DueDate=str(fake.date_time_this_year(after_now=False, tzinfo=None))
    ShipDate=str(fake.date_time_this_year(after_now=False, tzinfo=None))
    Status=str(random.choice(Statuss)[0])
    OnlineOrderFlag=str(random.choice(OnlineOrderFlags)[0])
    SalesOrderNumber=str(fake.isbn10(separator="-"))
    PurchaseOrderNumber=str(fake.isbn13(separator="-"))
    AccountNumber=str(random.choice(AccountNumbers)[0])
    CustomerID=str(random.choice(CustomerIDs)[0])
    ContactID=str(random.choice(ContactIDs)[0])
    SalesPersonID=str(random.choice(SalesPersonIDs)[0])
    TerritoryID=str(random.choice(TerritoryIDs)[0])
    BillToAddressID=str(random.choice(BillToAddressIDs)[0])
    ShipToAddressID=str(random.choice(ShipToAddressIDs)[0])
    ShipMethodID=str(random.choice(ShipMethodIDs)[0])
    CreditCardID=str(random.choice(CreditCardIDs)[0])
    CreditCardApprovalCode=fake.credit_card_security_code(card_type=None)
    CurrencyRateID=str(random.choice(CurrencyRateIDs)[0])
    SubTotal=random.random()*300
    TaxAmt=SubTotal*random.random()*20
    Freight=SubTotal*random.random()*30
    TotalDue=SubTotal+TaxAmt+Freight
    Comment=fake.paragraph(nb_sentences=1, variable_nb_sentences=True, ext_word_list=None)
    #rowguid=random.choice(rowguids)[0]
    ModifiedDate="now()"
    mysql="INSERT INTO salesorderheader (SalesOrderID,RevisionNumber,OrderDate,DueDate,ShipDate,Status,OnlineOrderFlag,SalesOrderNumber,PurchaseOrderNumber,AccountNumber,CustomerID,ContactID,SalesPersonID,TerritoryID,BillToAddressID,ShipToAddressID,ShipMethodID,CreditCardID,CreditCardApprovalCode,CurrencyRateID,SubTotal,TaxAmt,Freight,TotalDue,Comment,rowguid,ModifiedDate) VALUES ("
    mysql+="'"+SalesOrderID+"',"
    mysql+="'"+RevisionNumber+"',"
    mysql+="("+OrderDate+"),"
    mysql+="'"+DueDate+"',"
    mysql+="'"+ShipDate+"',"
    mysql+="'"+Status+"',"
    mysql+=""+OnlineOrderFlag+","
    mysql+="'"+SalesOrderNumber+"',"
    mysql+="'"+PurchaseOrderNumber+"',"
    mysql+="'"+AccountNumber+"',"
    mysql+="'"+CustomerID+"',"
    mysql+="'"+ContactID+"',"
    mysql+="'"+SalesPersonID+"',"
    mysql+="'"+TerritoryID+"',"
    mysql+="'"+BillToAddressID+"',"
    mysql+="'"+ShipToAddressID+"',"
    mysql+="'"+ShipMethodID+"',"
    mysql+="'"+CreditCardID+"',"
    mysql+="'"+CreditCardApprovalCode+"',"
    mysql+="'"+CurrencyRateID+"',"
    mysql+="'"+str(SubTotal)+"',"
    mysql+="'"+str(TaxAmt)+"',"
    mysql+="'"+str(Freight)+"',"
    mysql+="'"+str(TotalDue)+"',"
    mysql+="'"+Comment+"',"
    mysql+="'"+"xxx"+"',"
    mysql+="("+ModifiedDate+")"
    mysql+=");"

    #print(mysql)
    cursor = connection.cursor()
    cursor.execute(mysql)
    connection.commit()
    time.sleep(args.sleep)
