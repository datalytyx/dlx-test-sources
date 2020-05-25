# Wait to be sure that SQL Server came up
sleep 90s

# Run the setup script to create the DB and the schema in the DB
# Note: make sure that your password matches what is in the Dockerfile

find /database -type f -name "*.sql" | while read file ; do
  /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P $PASSWORD -d master -i $file
done
