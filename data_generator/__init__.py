import argparse
import sys

import colorlog

logger = colorlog.getLogger('data-generator')


def __init_logger():
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname)s:%(name)s:%(message)s'))
    logger.addHandler(handler)


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
                        help='Sleep in seconds between row inserts')
    parser.add_argument('--remove-ids-above',
                        type=str,
                        dest='ids',
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
            logger.error(f"Password must tbe specified by -t or --table if using db-type {args.db_type}")
            command_error = True

    if args.db_type == 'mssql' or 'postresql' or 'oracle':
        if not args.schema:
            logger.error(f"Schema must tbe specified by -s or --schema if using db-type {args.db_type}")
            command_error = True

    if command_error:
        sys.exit(1)


if __name__ == '__main__':
    main()
