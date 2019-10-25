# Introduction

The code in the repository was designed to create a large number of test databases spanning a wide range of use cases, databases and version numbers to test various ingestion capabilities including full, incremental and log based replication. Below are a list of all of the different databases currently supported and which databases are on them. Note due to minor differences in DB engines, the same database is NOT guarenteed to be the same across different databases or database versions. The number in parentheses is the port number that instance will be listening on.

| database_engine | version  | adventureworks | sakila   | world_x | siemens-test-db | jaffle |  hopper |
| -------- | -------- | --------       | -------- |-------- |--------         |-------- |-------- |
| mysql | 5.5 | Y (4000) | N | Y (4200) | Y (4300) | N (just needs config tweak) | Y (4501) |
| mysql | 5.6 | Y (4001) | Y (4101) | Y (4201) | Y (4301) | Y (4401) |Y (4501) |
| mysql | 5.7 | Y (4002) | Y (4102) | Y (4202) | Y (4302) | Y (4402) |Y (4501) |
| mysql | 8.0 | Y (4003) | Y (4103) | Y (4203) | Y (4203) | N (just needs config tweak) |Y (4501) |
| mariadb | 10.1 | Y (5000) | N | N | N | N |
| mariadb | 10.2 | Y (5001) | N | N | N | N |
| mariadb | 10.3 | Y (5002) | N | N | N | N |
| mariadb | 10.4 | Y (5003) | N | N | N | N |
| postgres | 9.6 | Y (6000) | Y  (6100) | N | N | N |
| MS SQL Server (Linux) | 2017 | Y (7000) | N | N | N | N |
| MS SQL Server (Linux) | 2019 | Y (7001) | N | N | N | N |


# Installation/envrionment
This has ONLY been tested running on an Ubuntu 18.04 and a recent version of docker. Further setup is required for incremental loads (see below).

To create ALL the databases in the table above simply run:

```
./create_all_databases.sh
```

Or just run individual commands/sections from this script. NOTE: the execution is VERY sensitive to being in the right folder. If you are not, everything will probably run fine but you'll end up with an empty database. If you find data not loading it's very likely this is the cause.

Most of these have been able to be achieved without creating specific docker images for each one, rather official docker images are used and data/scripts are injected. However the mssql ones require custom docker images to be built (done by the script). Note that these use mssql-linux and no warranties are made about it's compatibility with mssql on Windows. https://www.dbbest.com/blog/running-sql-server-on-linux/ has a good summary of what is not supported on the linux version.

The (currently incomplete) script

```
./show_summary_add_databases.sh
```

Is intended to run a command against each instance to confirm that data has been loaded correctly. For all the mysql based databases this is a query against the information schema. Note that row counts in here are estimates only - it will look like some data has not completely loaded but usually this is just bad estimation. If you are in any doubt check with a proper ```select count(*) ```.

The script:

```
./remove_all_databases.sh
```

Should remove all the created databases.

# Setting up incremental loads
To test various key and log based incremental replications, it is necessary to continuously add in new rows of data. A data generator has been created that can create up to 100 new rows per second (perhaps more on a higher spec machine). Currently it only adds new rows to the ```salesorderheader``` table in the ```adventureworks```  database. It currently only works with the following databases:
* mysql5.5
* mysql5.6
* mysql5.7
* mysql8.0
* mariadb10.1
* mariadb10.2
* mariadb10.3
* mariadb10.4

The load scripts current run from the host, not inside a container. To prepare your system for them ensure you are running python3 and then:

```
pip3 install Faker mysql-connector-python
```


Run once for each database you want to insert to:
```
python3 -u adventureworks_incremental_data_generator.py  --host 127.0.0.1 --port 4000 --database adventureworks --username datalytyx --password horsewelltree --sleep 0 > /tmp/mysql_incremental_4000.log 2>&1 &

python3 -u adventureworks_incremental_data_generator.py  --host 127.0.0.1 --port 4001 --database adventureworks --username datalytyx --password horsewelltree --sleep 0 > /tmp/mysql_incremental_4001.log 2>&1 &

python3 -u adventureworks_incremental_data_generator.py  --host 127.0.0.1 --port 4002 --database adventureworks --username datalytyx --password horsewelltree --sleep 0 > /tmp/mysql_incremental_4002.log 2>&1 &

python3 -u adventureworks_incremental_data_generator.py  --host 127.0.0.1 --port 4003 --database adventureworks --username datalytyx --password horsewelltree --sleep 0 > /tmp/mysql_incremental_4003.log 2>&1 &

python3 -u adventureworks_incremental_data_generator.py  --host 127.0.0.1 --port 5000 --database adventureworks --username datalytyx --password horsewelltree --sleep 0 > /tmp/mysql_incremental_5000.log 2>&1 &

python3 -u adventureworks_incremental_data_generator.py  --host 127.0.0.1 --port 5001 --database adventureworks --username datalytyx --password horsewelltree --sleep 0 > /tmp/mysql_incremental_5001.log 2>&1 &

python3 -u adventureworks_incremental_data_generator.py  --host 127.0.0.1 --port 5002 --database adventureworks --username datalytyx --password horsewelltree --sleep 0 > /tmp/mysql_incremental_5002.log 2>&1 &

python3 -u adventureworks_incremental_data_generator.py  --host 127.0.0.1 --port 5003 --database adventureworks --username datalytyx --password horsewelltree --sleep 0 > /tmp/mysql_incremental_5003.log 2>&1 &
```

Note the port number changing to point to different database_engine/dataset combinations. These scripts are not robust - loosing connection to their DB will cause them to exit. If you need them to be reliable, wrap them in a restart loop.
