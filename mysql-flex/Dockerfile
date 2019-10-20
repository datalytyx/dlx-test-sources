FROM mysql:5.7.28

COPY mysqld.cnf /etc/mysql/my.cnf

# Import AdventureWorks database
ADD AdventureWorks.sql.tar.gz /tmp/
RUN echo "Installing database"
RUN ls -l /var/lib/mysql
RUN service mysql start && mysql < /tmp/AdventureWorks.sql && mysql -e 'GRANT ALL ON *.* TO datalytyx@"%" IDENTIFIED BY "horsewelltree";' && service mysql stop
RUN ls -l /var/lib/mysql
RUN service mysql start && mysql -e 'show databases';

RUN touch /aaaaa

ENTRYPOINT service mysql start && tail -f /var/log/mysql/*.log

EXPOSE 3306
