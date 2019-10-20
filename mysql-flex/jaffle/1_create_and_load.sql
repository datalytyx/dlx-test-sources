drop table if exists raw_customers ;
create table raw_customers ( id int, first_name varchar(200), last_name varchar(200), email varchar(200) );
drop table if exists raw_orders;
create table raw_orders ( id int, user_id int, order_date DATE, status varchar(200) );
drop table if exists raw_payments;
create table raw_payments ( id int, order_id int, payment_method varchar(200), amount int );

LOAD DATA INFILE '/var/lib/mysql-files/raw_customers.csv'  INTO TABLE raw_customers  FIELDS TERMINATED BY ','  ENCLOSED BY '' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA INFILE '/var/lib/mysql-files/raw_orders.csv'  INTO TABLE raw_orders  FIELDS TERMINATED BY ','  ENCLOSED BY '' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA INFILE '/var/lib/mysql-files/raw_payments.csv'  INTO TABLE raw_payments  FIELDS TERMINATED BY ','  ENCLOSED BY '' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
