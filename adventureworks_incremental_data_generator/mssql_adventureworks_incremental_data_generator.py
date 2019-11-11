from faker import Faker
import time
import argparse
import random
import pymssql

parser = argparse.ArgumentParser(description='MsSQL AdventureWorks Incremental Data Generator')
parser.add_argument('--host', metavar='host', help='mssql ip address')
parser.add_argument('--port', metavar='N',    help='mssql port')
parser.add_argument('--database', metavar='N', help='mssql database')
parser.add_argument('--username', metavar='N', help='mssql username')
parser.add_argument('--password', metavar='N', help='mssql password')
parser.add_argument('--type', metavar='N', help='mysql|mssql')
parser.add_argument('--sleep', type=float, metavar='N', default=0, help='sleep seconds between row inserts')
parser.add_argument('--schema', type=str, metavar='N', help='mssql schema name')
parser.add_argument('--removeidsabove', type=str, metavar='N', help='Delete all rows with a SalesOrderId over this value')

args = parser.parse_args()
SCHEMA = ''

try:
    if (args.type == 'mysql'):
        pass
        SCHEMA = ""
        connection = mysql.connector.connect(host=args.host,port=args.port,database=args.database,user=args.username,password=args.password)
        cursor = connection.cursor()
    elif (args.type == 'mssql'):
        connection = pymssql.connect(server=args.host, user=args.username, password=args.password, database=args.database, port=args.port)
        cursor = connection.cursor()
        SCHEMA = f'{args.schema}.'
    else:
      print("--type not set to either mysql or mssql")
      exit()
    if (args.removeidsabove):
      query=f"delete from {SCHEMA}salesorderheader where SalesOrderId > "+args.removeidsabove
      print("Truncating salesorderheader using: "+query)
      cursor.execute(query)
      connection.commit()


    query=f"select max(SalesOrderID) from {SCHEMA}salesorderheader"
    cursor.execute(query)
    MaxSalesOrderID=cursor.fetchone()[0]
    print("Found max(SalesOrderID): "+str(MaxSalesOrderID))

    print("Getting CustomerIDs")
    cursor.execute(f"select distinct(CustomerID) from {SCHEMA}salesorderheader where CustomerID is not null order by 1")
    CustomerIDs=cursor.fetchall()
    print("Getting Status")
    cursor.execute(f"select distinct(Status) from {SCHEMA}salesorderheader where Status is not null order by 1")
    Statuss=cursor.fetchall()
    print("Getting OnlineOrderFlag")
    cursor.execute(f"select distinct(OnlineOrderFlag) from {SCHEMA}salesorderheader where OnlineOrderFlag is not null order by 1")
    OnlineOrderFlags=cursor.fetchall()
    cursor.execute(f"select distinct(AccountNumber) from {SCHEMA}salesorderheader where AccountNumber is not null order by 1")
    AccountNumbers=cursor.fetchall()
    cursor.execute(f"select distinct(TerritoryID) from {SCHEMA}salesorderheader where TerritoryID is not null order by 1")
    TerritoryIDs=cursor.fetchall()
    cursor.execute(f"select distinct(BillToAddressID) from {SCHEMA}salesorderheader where BillToAddressID is not null order by 1")
    BillToAddressIDs=cursor.fetchall()
    cursor.execute(f"select distinct(ShipToAddressID) from {SCHEMA}salesorderheader where ShipToAddressID is not null order by 1")
    ShipToAddressIDs=cursor.fetchall()
    cursor.execute(f"select distinct(ShipMethodID) from {SCHEMA}salesorderheader where ShipMethodID is not null order by 1")
    ShipMethodIDs=cursor.fetchall()
    cursor.execute(f"select distinct(CreditCardID) from {SCHEMA}salesorderheader where CreditCardID is not null order by 1")
    CreditCardIDs=cursor.fetchall()
    cursor.execute(f"select distinct(CurrencyRateID) from {SCHEMA}salesorderheader where CurrencyRateID is not null order by 1")
    CurrencyRateIDs=cursor.fetchall()
    cursor.execute(f"select distinct(SalesPersonID) from {SCHEMA}salesorderheader where SalesPersonID is not null order by 1")
    SalesPersonIDs=cursor.fetchall()


except Exception as e:
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
    OrderDate="getdate()"
    DueDate="getdate() + 1"
    ShipDate="getdate() + 1"
    Status=str(random.choice(Statuss)[0])
    OnlineOrderFlag=str(random.choice(OnlineOrderFlags)[0])
    SalesOrderNumber = "dss" + str(SalesOrderID)
    PurchaseOrderNumber=str(fake.isbn13(separator="-"))
    AccountNumber=str(random.choice(AccountNumbers)[0])
    CustomerID=str(random.choice(CustomerIDs)[0])
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
    ModifiedDate="getdate()"
    mysql=f"INSERT INTO {SCHEMA}salesorderheader (RevisionNumber,OrderDate,DueDate,ShipDate,Status,PurchaseOrderNumber,AccountNumber,CustomerID,SalesPersonID,TerritoryID,BillToAddressID,ShipToAddressID,ShipMethodID,CreditCardID,CreditCardApprovalCode,CurrencyRateID,SubTotal,TaxAmt,Freight,Comment,ModifiedDate) VALUES ("
    mysql+="'"+RevisionNumber+"',"
    mysql+="("+OrderDate+"),"
    mysql+="("+DueDate+"),"
    mysql+="("+ShipDate+"),"
    mysql+="'"+Status+"',"
    mysql+="'"+PurchaseOrderNumber+"',"
    mysql+="'"+AccountNumber+"',"
    mysql+="'"+CustomerID+"',"
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
    mysql+="'"+Comment+"',"
    mysql+="("+ModifiedDate+")"
    mysql+=");"

    #print(mysql)
    cursor = connection.cursor()
    cursor.execute(mysql)
    connection.commit()
    time.sleep(args.sleep)
