import csv
from jinja2 import Template
import argparse


def main():
    parser = argparse.ArgumentParser(description='AdventureWorks Incremental Data Generator')
    parser.add_argument('--source', metavar='config csv file', required=True,
                        help='csv file in the format source_db_engine,db_version,source,port with a header row ')
    parser.add_argument('--action', metavar='template', required=True, help='template file defining action')
    parser.add_argument('--regex', metavar='regex string', help='regex to apply to source (NOT YET IMPLEMENTED)')

    args = parser.parse_args()

    template = Template(open(args.action, 'r').read())

    reader = csv.reader(open(args.source, 'r'))
    next(reader, None)  # ignore the header row

    sources = []
    for row in reader:
        source_element = {'source_db_engine': row[0], 'source_db_version': row[1], 'source_path': row[2],
                          'source_data': row[3], 'port': row[4]}
        source_element['source_db_version_raw'] = source_element['source_db_version'].replace('.', '')
        sources.append(source_element)

    rendered_tap = template.render(sources=sources)
    print(rendered_tap)


if __name__ == '__main__':
    main()
