#!/bin/bash
/opt/mssql/bin/sqlservr --accept-eula & 
sleep 30 
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'Horsewelltr33' -i instawdb.sql
bash
