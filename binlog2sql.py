#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import pymysql
from pymysqlreplication import BinLogStreamReader
from binlog2sql_util import (
    command_line_args,
    is_dml_event,
    is_ddl_event,
    generate_sql,
    type_convert,
    write_file,
)
import json
import arrow


class Binlog2sql(object):

    def __init__(self, connection_settings, start_file=None, start_pos=None, end_file=None, end_pos=None,
                 start_time=None, stop_time=None, only_schemas=None, only_tables=None, no_pk=False,
                 flashback=False, stop_never=False, output_file='', only_dml=True,
                 sql_type=None, json=False):
        """
        conn_setting: {'host': 127.0.0.1, 'port': 3306, 'user': user, 'passwd': passwd, 'charset': 'utf8'}
        """

        # connect setting
        self.conn_setting = connection_settings

        # interval filter
        if not start_file:
            raise ValueError('Lack of parameter: start_file')
        self.start_file = start_file
        self.end_file = end_file or start_file
        self.start_pos = start_pos or 4  # use binlog v4
        self.end_pos = end_pos
        self._tzinfo = arrow.now().tzinfo
        self.start_time = start_time and arrow.get(
            arrow.get(start_time).astimezone(self._tzinfo)).timestamp or arrow.get().min.timestamp
        self.stop_time = stop_time and arrow.get(
            arrow.get(stop_time).astimezone(self._tzinfo)).timestamp or arrow.get().max.timestamp

        # schema filter
        self.only_schemas = only_schemas
        self.only_tables = only_tables

        # type filter
        self.only_dml = only_dml
        self.sql_type = sql_type and [t.upper() for t in sql_type] or ['INSERT', 'UPDATE', 'DELETE']

        # optional
        self.no_pk, self.flashback, self.stop_never, self.output_file, self.json = (
            no_pk, flashback, stop_never, output_file, json
        )

        self.connection = pymysql.connect(**self.conn_setting)
        with self.connection as cursor:
            cursor.execute("SHOW MASTER LOGS")
            for row in cursor.fetchall():
                if self.start_file <= row[0] <= self.end_file:
                    end_file, end_pos = row
            if end_file:
                self.end_file, self.end_pos = end_file, end_pos
            else:
                raise ValueError('parameter error: start_file %s not in mysql server' % self.start_file)

            cursor.execute("SELECT @@server_id")
            self.server_id = cursor.fetchone()[0]
            if not self.server_id:
                raise ValueError('missing server_id in %s:%s' % (self.conn_setting['host'], self.conn_setting['port']))

    def process_binlog(self):
        stream = BinLogStreamReader(connection_settings=self.conn_setting, server_id=self.server_id,
                                    log_file=self.start_file, log_pos=self.start_pos, only_schemas=self.only_schemas,
                                    only_tables=self.only_tables, resume_stream=True, blocking=True)
        with self.connection as cursor:
            sql = '# {} #\n# {} binlog2sql start! #\n# {} #'.format(''.ljust(50, '='), arrow.now(), ''.ljust(50, '='))
            print(sql)
            if self.output_file:
                write_file(self.output_file, sql)

            start_pos, print_interval, print_time = 4, 60 * 10, 0
            for binlog_event in stream:

                if (print_time + print_interval) < binlog_event.timestamp < self.start_time:
                    print_time = binlog_event.timestamp
                    print('# Binlog scan to {}'.format(arrow.get(print_time).to(self._tzinfo)))

                if binlog_event.timestamp < self.start_time:
                    continue

                # dml
                if is_dml_event(binlog_event):
                    for row in binlog_event.rows:
                        if self.json:
                            for column in binlog_event.columns:
                                if column.type == 245:
                                    for k, v in row.items():
                                        row[k][column.name] = json.dumps(type_convert(v[column.name]),
                                                                         ensure_ascii=False)
                        sql = generate_sql(cursor=cursor, binlog_event=binlog_event, no_pk=self.no_pk, row=row,
                                           e_start_pos=start_pos, flashback=self.flashback)
                        print(sql)
                        if self.output_file:
                            write_file(self.output_file, sql)
                    # ddl
                elif is_ddl_event(binlog_event):
                    start_pos = binlog_event.packet.log_pos
                    if not self.only_dml and binlog_event.query != 'BEGIN':
                        sql = generate_sql(cursor=cursor, binlog_event=binlog_event, no_pk=self.no_pk,
                                           e_start_pos=start_pos, flashback=self.flashback)
                        print(sql)
                        if self.output_file:
                            write_file(self.output_file, sql)

                # exceed the end position of the end binlog file
                if stream.log_file == self.end_file and (
                        binlog_event.packet.log_pos >= self.end_pos or binlog_event.timestamp >= self.stop_time) and not self.stop_never:
                    sql = '# {} #\n# {} binlog2sql stop!  #\n# {} #'.format(''.ljust(50, '='), arrow.now(),
                                                                            ''.ljust(50, '='))
                    print(sql)
                    if self.output_file:
                        write_file(self.output_file, sql)
                    break
            stream.close()


if __name__ == '__main__':
    args = command_line_args(sys.argv[1:])
    conn_setting = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password, 'charset': 'utf8'}
    binlog2sql = Binlog2sql(connection_settings=conn_setting, start_file=args.start_file, start_pos=args.start_pos,
                            end_file=args.end_file, end_pos=args.end_pos, start_time=args.start_time,
                            stop_time=args.stop_time, only_schemas=args.databases, only_tables=args.tables,
                            no_pk=args.no_pk, flashback=args.flashback, stop_never=args.stop_never,
                            output_file=args.output_file, only_dml=args.only_dml, sql_type=args.sql_type,
                            json=args.json)
    # conn_setting = {'host': '127.0.0.1', 'port': 3306, 'user': 'root', 'passwd': '123100', 'charset': 'utf8'}
    # binlog2sql = Binlog2sql(connection_settings=conn_setting, start_file='mysql-bin.000003',
    #                         end_file='mysql-bin.000003', stop_time='2018-12-26', json=True, flashback=True,
    #                         only_dml=True, output_file='backup.sql')
    binlog2sql.process_binlog()
