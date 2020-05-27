RESTORE DATABASE AdventureWorks2017
   FROM DISK = '/database/AdventureWorks2017.bak'
   WITH MOVE 'AdventureWorks2017' TO '/database/AdventureWorks2017.mdf',
   MOVE 'AdventureWorks2017_Log' TO '/database/AdventureWorks2017.ldf';
