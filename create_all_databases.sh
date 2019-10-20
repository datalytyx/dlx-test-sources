#!/bin/bash

cd mysql-flex
SOURCE=adventureworks
DB_VERSION=5.5 && PORT=4000 
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME

DB_VERSION=5.6 && PORT=4001
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME

DB_VERSION=5.7 && PORT=4002
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME

DB_VERSION=8.0 && PORT=4003
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME


SOURCE=sakila
# 5.5 does not supprt FULL TEST INDEXES
#DB_VERSION=5.5 && PORT=4100
#CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME

DB_VERSION=5.6 && PORT=4101 
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME

DB_VERSION=5.7 && PORT=4102
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME

DB_VERSION=8.0 && PORT=4103
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME


SOURCE=world_x
DB_VERSION=5.5 && PORT=4200
## SQL DOESN'T WORK PRE 5.7

DB_VERSION=5.6 && PORT=4201 
## SQL DOESN'T WORK PRE 5.7

DB_VERSION=5.7 && PORT=4202
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME

DB_VERSION=8.0 && PORT=4203
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME


SOURCE=siemens-test-db
DB_VERSION=5.5 && PORT=4300
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME

DB_VERSION=5.6 && PORT=4301 
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME

DB_VERSION=5.7 && PORT=4302
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME

DB_VERSION=8.0 && PORT=4303
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME


SOURCE=jaffle
### NOTE : extra mount point to put csv files in a place mysql can read from without needing insecure mode
DB_VERSION=5.5 && PORT=4400
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d -v $(pwd)/$SOURCE:/var/lib/mysql-files/ $IMAGE_NAME
DB_VERSION=5.6 && PORT=4401
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d -v $(pwd)/$SOURCE:/var/lib/mysql-files/ $IMAGE_NAME
DB_VERSION=5.7 && PORT=4402
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d -v $(pwd)/$SOURCE:/var/lib/mysql-files/ $IMAGE_NAME
DB_VERSION=8.0 && PORT=4403
CONTAINER_NAME=mysql$DB_VERSION-$SOURCE && IMAGE_NAME=mysql:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d -v $(pwd)/$SOURCE:/var/lib/mysql-files/ $IMAGE_NAME


SOURCE=adventureworks
# Mariadb containers before 10.4 don't appear to have the automated schema load hooks in their entrypoint
DB_VERSION=10.1 && PORT=5000 
CONTAINER_NAME=mariadb$DB_VERSION-$SOURCE && IMAGE_NAME=mariadb:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME

DB_VERSION=10.2 && PORT=5001 
CONTAINER_NAME=mariadb$DB_VERSION-$SOURCE && IMAGE_NAME=mariadb:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME

DB_VERSION=10.3 && PORT=5002 
CONTAINER_NAME=mariadb$DB_VERSION-$SOURCE && IMAGE_NAME=mariadb:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME

DB_VERSION=10.4 && PORT=5003
CONTAINER_NAME=mariadb$DB_VERSION-$SOURCE && IMAGE_NAME=mariadb:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e MYSQL_USER=datalytyx -e MYSQL_PASSWORD=horsewelltree -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_DATABASE=$SOURCE -p $PORT:3306 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME


cd ..
cd postgres-flex

SOURCE=adventureworks
DB_VERSION=9.6 && PORT=6000 
CONTAINER_NAME=postgres$DB_VERSION-$SOURCE && IMAGE_NAME=postgres:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e POSTGRES_USER=datalytyx -e POSTGRES_PASSWORD=horsewelltree -e POSTGRES_DB=datalytyx -p $PORT:5432 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME



SOURCE=sakila
DB_VERSION=9.6 && PORT=6100 
CONTAINER_NAME=postgres$DB_VERSION-$SOURCE && IMAGE_NAME=postgres:$DB_VERSION && docker run -d  --name $CONTAINER_NAME -e POSTGRES_USER=datalytyx -e POSTGRES_PASSWORD=horsewelltree -e POSTGRES_DB=datalytyx -p $PORT:5432 -v $(pwd)/$SOURCE:/docker-entrypoint-initdb.d $IMAGE_NAME

cd ..


cd mssql-2017
docker build -t datalytyx:mssql2017-adventureworks .
docker run -e 'SA_PASSWORD=Horsewelltr33' -p 7000:1433 -d -it --name mssql2017-adventureworks datalytyx:mssql2017-adventureworks
cd ..



cd mssql-2019
docker build -t datalytyx:mssql2019-adventureworks .
docker run -e 'SA_PASSWORD=Horsewelltr33' -p 7001:1433 -d -it --name mssql2019-adventureworks datalytyx:mssql2019-adventureworks
cd ..

