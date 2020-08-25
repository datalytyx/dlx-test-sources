import argparse
import logging
import sys
import time

import colorlog
from faker import Faker

from data_generator.kafka_generator import Kafka
from data_generator.mssql_generator import MSSQL
from data_generator.mysql_generator import MySQL
from data_generator.oracle_generator import Oracle
from data_generator.postgresql_generator import PostgreSQL

logger = colorlog.getLogger('data-generator')


def __init_logger():
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname)s:%(name)s:%(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def main():
    __init_logger()

    parser = argparse.ArgumentParser(description='Incremental Data Generator')
    parser.add_argument('-H', '--host',
                        type=str,
                        dest='host',
                        default='localhost',
                        required=True,
                        help='Database IP Address')
    parser.add_argument('-p', '--port',
                        type=int,
                        dest='port',
                        help='Database Post')
    parser.add_argument('--db-type', '-d',
                        type=str,
                        dest='db_type',
                        required=True,
                        choices=['mysql', 'mssql', 'postgresql', 'oracle', 'kafka'],
                        help='Type of the database. '
                             'Use one of \'mysql\', \'mssql\', \'postresql\', \'oracle\' or \'kafka\'')
    parser.add_argument('--database', '-D',
                        type=str,
                        dest='database',
                        required=True,
                        help='Database name. SID in case of Oracle and Topic name in case of Kafka')
    parser.add_argument('--username', '-U',
                        type=str,
                        dest='username',
                        default=None,
                        help='Database user/role with read and write privileges on the specified table')
    parser.add_argument('--password', '-P',
                        type=str,
                        dest='password',
                        default=None,
                        help='Password for the specified user/role')
    parser.add_argument('--insertion-type',
                        type=str,
                        dest='type',
                        default='incremental',
                        choices=['incremental', 'create'],
                        help='Type of insertion to be made in kafka topic(New row or Incremental)')
    parser.add_argument('--table', '-t',
                        type=str,
                        dest='table',
                        default=None,
                        help='Name of the table in which data has to be inserted')
    parser.add_argument('--schema', '-s',
                        type=str,
                        dest='schema',
                        default=None,
                        help='Name of the schema in which the specified table exists')
    parser.add_argument('--sleep',
                        type=int,
                        dest='sleep',
                        default=0,
                        help='Sleep in seconds between row inserts')
    parser.add_argument('--remove-ids-above',
                        type=str,
                        dest='ids',
                        default=None,
                        help='Delete all rows with a id over this value')
    parser.add_argument('--time-shift-to-now',
                        dest='time_shift',
                        action="store_true")
    parser.add_argument('--counter', '-C',
                        type=int,
                        dest='counter',
                        default=100,
                        help='Counter after which to display number of inserted rows')
    parser.add_argument('--rows', '-R',
                        type=int,
                        dest='rows',
                        default=0,
                        help='Rows to be inserted in Topic')
    args = parser.parse_args()

    command_error = False

    # Command specific argument validations
    if args.db_type != 'kafka':
        if not args.port:
            logger.error(f"Port must tbe specified by -p or --port if using db-type {args.db_type}")
            command_error = True
        if not args.username:
            logger.error(f"Username must tbe specified by -U or --username if using db-type {args.db_type}")
            command_error = True
        if not args.password:
            logger.error(f"Password must tbe specified by -P or --password if using db-type {args.db_type}")
            command_error = True
        if not args.table:
            logger.error(f"Table must tbe specified by -t or --table if using db-type {args.db_type}")
            command_error = True

    if (args.db_type == 'postgresql') or (args.db_type == 'oracle'):
        if not args.schema:
            logger.error(f"Schema must tbe specified by -s or --schema if using db-type {args.db_type}")
            command_error = True

    if args.counter < 1:
        logger.error("Counter value cannot be less than 1")
        command_error = True

    if args.sleep < 0:
        logger.error("Sleep value cannot be less than 0")
        command_error = True

    if command_error:
        sys.exit(1)

    db = None
    if args.db_type == 'mysql':
        db = MySQL(args, logger)
    elif args.db_type == 'mssql':
        db = MSSQL(args, logger)
    elif args.db_type == 'postgresql':
        db = PostgreSQL(args, logger)
    elif args.db_type == 'oracle':
        db = Oracle(args, logger)
    elif args.db_type == 'kafka':
        db = Kafka(args, logger)

    columns = db.get_column_values()
    loop_counter = 0
    while True and (args.rows == 0 or loop_counter < args.rows):
        loop_counter += 1
        if (loop_counter % args.counter) == 0:
            log = f"Inserted {loop_counter} rows"
            if not args.rows:
                log += f" out of {args.rows}"
            logger.info(log)
        fake = Faker()
        fake.seed_instance(loop_counter)  # by using the loop counter as a seed, the data is both random, but repeatable
        row = db.set_column_values(columns, loop_counter, fake)
        sql_query = db.generate_query(row)
        # print(sql_query)
        db.insert_and_commit(sql_query)
        time.sleep(args.sleep)


if __name__ == '__main__':
    main()
