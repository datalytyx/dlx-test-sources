#!/bin/bash


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

