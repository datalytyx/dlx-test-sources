#!/bin/bash
for PORT in 4000 4001 4002 4003 4100 4101 4102 4102 4202 4203 4300 4301 4302 4303 5000 5001 5002 5003 4400 4401 4402 4403 4500 4501 4502 4503 5500 5501 5502 5503
do	
	echo ===================
	echo $PORT
	mysql --host 127.0.0.1 --port $PORT --user=datalytyx --password=horsewelltree -e "SELECT table_name, table_rows,table_schema FROM INFORMATION_SCHEMA.TABLES where table_schema not in  ('information_schema','sys','performance_schema','mysql');"
done

