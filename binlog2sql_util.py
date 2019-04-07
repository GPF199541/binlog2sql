#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
import platform
import argparse
import arrow
import getpass
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)

PY_VERSION = platform.python_version()

if PY_VERSION > '3':
    PY3PLUS = True
else:
    PY3PLUS = False


def parse_args():
    """parse args for binlog2sql"""

    parser = argparse.ArgumentParser(description='Parse MySQL binlog to SQL you want', add_help=False)
    # connect setting
    connect = parser.add_argument_group('connect setting')
    connect.add_argument('-h', '--host', dest='host', type=str, help='MySQL host', default='127.0.0.1')
    connect.add_argument('-P', '--port', dest='port', type=int, help='MySQL port', default=3306)
    connect.add_argument('-u', '--user', dest='user', type=str, help='MySQL user', default='root')
    connect.add_argument('-p', '--password', dest='password', type=str, nargs='*', help='MySQL password', default='')

    # interval filter
    interval = parser.add_argument_group('interval filter')
    interval.add_argument('--start-file', dest='start_file', type=str, help='Start binlog file to be parsed')
    interval.add_argument('--stop-file', dest='stop_file', type=str,
                          help="Stop binlog file to be parsed. default: '--start-file'")
    interval.add_argument('--start-position', dest='start_position', type=int,
                          help='Start position of the --start-file')
    interval.add_argument('--stop-position', dest='stop_position', type=int,
                          help="Stop position. default: latest position of '--stop-file'")
    interval.add_argument('--start-time', dest='start_time', type=str, help="Start time. format yyyy-MM-dd[ hh:mm:ss]")
    interval.add_argument('--stop-time', dest='stop_time', type=str, help="Stop Time. format yyyy-MM-dd[ hh:mm:ss]")

    # schema filter
    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--databases', dest='databases', type=str, nargs='*', help='dbs you want to process')
    schema.add_argument('-t', '--tables', dest='tables', type=str, nargs='*', help='tables you want to process')

    # type filter
    event = parser.add_argument_group('type filter')
    event.add_argument('--only-dml', dest='only_dml', action='store_true', default=False,
                       help='only print dml, ignore ddl')
    event.add_argument('--sql-type', dest='sql_type', type=str, nargs='*', default=['INSERT', 'UPDATE', 'DELETE'],
                       help='Sql type you want to process, support INSERT, UPDATE, DELETE.')

    # optional
    optional = parser.add_argument_group('optional')
    optional.add_argument('-K', '--no-primary-key', dest='no_pk', action='store_true',
                          help='Generate insert sql without primary key if exists', default=False)
    optional.add_argument('-B', '--flashback', dest='flashback', action='store_true',
                          help='Flashback data to start_position of start_file', default=False)
    optional.add_argument('--stop-never', dest='stop_never', action='store_true', default=False,
                          help="Continuously parse binlog. default: stop at the latest event when you start.")
    optional.add_argument('--output-file', dest='output_file', default='', help='Write SQL to output file')
    optional.add_argument('--json', dest='json', action='store_true', default=False,
                          help='Support MySQL 5.7 JSON type.')
    optional.add_argument('--help', dest='help', action='store_true', help='help information', default=False)
    optional.add_argument('--debug', dest='debug', action='store_true', help='debug, print all args', default=False)
    return parser


def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    if args.flashback and args.stop_never:
        raise ValueError('Only one of flashback or stop-never can be True')
    if args.flashback and args.no_pk:
        raise ValueError('Only one of flashback or no_pk can be True')
    if args.flashback and not args.only_dml:
        raise ValueError('DDL cannot be flashback')
    if (args.start_time and not is_valid_datetime(args.start_time)) or \
            (args.stop_time and not is_valid_datetime(args.stop_time)):
        raise ValueError('Incorrect datetime argument')
    if not args.password or not args.password[0]:
        args.password = getpass.getpass()
    else:
        args.password = args.password[0]
    return args


def is_valid_datetime(string):
    try:
        arrow.get(string)
        return True
    except:
        return False


def compare_items(items):
    # caution: if v is NULL, may need to process
    (k, v) = items
    if v is None:
        return '`%s` IS %%s' % k
    else:
        return '`%s`=%%s' % k


def type_convert(data):
    if PY3PLUS and isinstance(data, bytes):
        data = data.decode('utf-8')
    elif not PY3PLUS and isinstance(data, unicode):
        data = data.encode('utf-8')
    elif isinstance(data, dict):
        data = dict(map(type_convert, data.items()))
    elif isinstance(data, tuple):
        data = tuple(map(type_convert, data))
    elif isinstance(data, list):
        data = list(map(type_convert, data))
    return data


def is_dml_event(event):
    if isinstance(event, WriteRowsEvent):
        return 'INSERT'
    elif isinstance(event, UpdateRowsEvent, ):
        return 'UPDATE'
    elif isinstance(event, DeleteRowsEvent):
        return 'DELETE'
    else:
        return ''


def is_ddl_event(event):
    if isinstance(event, QueryEvent):
        return True
    else:
        return False


def generate_sql(cursor, binlog_event, row=None, e_start_pos=None, flashback=False, no_pk=False):
    if row:
        pattern = generate_sql_pattern(binlog_event, row=row, flashback=flashback, no_pk=no_pk)
        sql = cursor.mogrify(pattern['template'], pattern['values'])
    else:
        if binlog_event.schema:
            sql = 'USE {0};\n'.format(type_convert(binlog_event.schema))
        else:
            sql = ''
        sql += '{0};'.format(type_convert(binlog_event.query))
    time = arrow.get(binlog_event.timestamp).astimezone(arrow.now().timetz().tzinfo)
    sql += ' #start %s end %s time %s' % (e_start_pos, binlog_event.packet.log_pos, time)

    return sql


def generate_sql_pattern(binlog_event, row=None, flashback=False, no_pk=False):
    template = ''
    values = []
    if flashback is True:
        if isinstance(binlog_event, WriteRowsEvent):
            template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table,
                ' AND '.join(map(compare_items, row['values'].items()))
            )
            values = map(type_convert, row['values'].values())
        elif isinstance(binlog_event, DeleteRowsEvent):
            template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                ', '.join(['%s'] * len(row['values']))
            )
            values = map(type_convert, row['values'].values())
        elif isinstance(binlog_event, UpdateRowsEvent):
            template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(['`%s`=%%s' % x for x in row['before_values'].keys()]),
                ' AND '.join(map(compare_items, row['after_values'].items())))
            values = map(type_convert, list(row['before_values'].values()) + list(row['after_values'].values()))
    else:
        if isinstance(binlog_event, WriteRowsEvent):
            if no_pk:
                # print binlog_event.__dict__
                # tableInfo = (binlog_event.table_map)[binlog_event.table_id]
                # if tableInfo.primary_key:
                #     row['values'].pop(tableInfo.primary_key)
                if binlog_event.primary_key:
                    row['values'].pop(binlog_event.primary_key)

            template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                ', '.join(['%s'] * len(row['values']))
            )
            values = map(type_convert, row['values'].values())
        elif isinstance(binlog_event, DeleteRowsEvent):
            template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table, ' AND '.join(map(compare_items, row['values'].items())))
            values = map(type_convert, row['values'].values())
        elif isinstance(binlog_event, UpdateRowsEvent):
            template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                binlog_event.schema, binlog_event.table,
                ', '.join(['`%s`=%%s' % k for k in row['after_values'].keys()]),
                ' AND '.join(map(compare_items, row['before_values'].items()))
            )
            values = map(type_convert, list(row['after_values'].values()) + list(row['before_values'].values()))

    return {'template': template, 'values': list(values)}


def write_file(file, line):
    if PY3PLUS:
        with open(file, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    else:
        with open(file, 'a') as f:
            f.write(line + '\n')


def print_line(line, file=None):
    # if pycharm(utf-8)
    # print(line)

    # if windows cmd(gbk)
    if not PY3PLUS and platform.system() == 'Windows':
        print(line.decode('utf-8').encode('gbk'))
    else:
        print(line)
    if file:
        if PY3PLUS:
            with open(file, 'a', encoding='utf-8') as f:
                f.write(line + '\n')
        else:
            with open(file, 'a') as f:
                f.write(line + '\n')
