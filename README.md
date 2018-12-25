binlog2sql
===

从 `MySQL binlog` 中解析出 `REDO SQL, UNDO SQL`。本项目是[danfengcao/binlog2sql](https://github.com/danfengcao/binlog2sql)的分支，将长期更新并维护。  


用途
===

* `UNDO SQL` 用于数据恢复，例如误操作或业务异常数据。
* `REDO SQL` 用于数据重做，例如主从切换后 `MASTER` 丢失数据（延迟）。
* 生成标准 `SQL` 用于其它用途。


适用
===
* `Python 2.7, 3.4+`
* `MySQL 5.6, 5.7`


安装
===

```
shell> git clone https://github.com/nloneday/binlog2sql.git && cd binlog2sql
shell> pip install -r requirements.txt
```

权限
===

```
GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO `user`@`%`;

# 说明：
select：需要读取server端information_schema.COLUMNS表，获取表结构的元信息，拼接成可视化的sql语句
replication client：两个权限都可以，需要执行'SHOW MASTER STATUS', 获取server端的binlog列表
replication slave：通过BINLOG_DUMP协议获取binlog内容的权限
```


用法
===

- `REDO SQL, UNDO SQL` 的唯一区别是 `--flashback` 选项。

```bash
# 解析出重做 REDO SQL

shell> python binlog2sql.py -h127.0.0.1 -P3306 -uadmin -p \
       --start-file='mysql-bin.000003' --start-datetime='2018-12-24 14:00:00'

输出：
INSERT INTO `test`.`person`(`id`, `name`) VALUES (1, 'haha'); 
#start 568 end 803 time 2018-12-24 14:04:00
INSERT INTO `test`.`person`(`id`, `name`) VALUES (2, 'wawa'); 
#start 834 end 1069 time 2018-12-24 14:05:23

```

```bash
# 解析出回滚 UNDO SQL

shell> python binlog2sql.py -h127.0.0.1 -P3306 -uadmin -p \
       --start-file='mysql-bin.000003' --start-datetime='2018-12-24 14:00:00' \
       --flashback

输出：
DELETE FROM `test`.`person` WHERE `id`=2 AND `name`='wawa' LIMIT 1; 
#start 834 end 1069 time 2018-12-24 14:05:23
DELETE FROM `test`.`person` WHERE `id`=1 AND `name`='haha' LIMIT 1; 
#start 568 end 803 time 2018-12-24 14:04:00

```

- `MySQL 5.7` 新增 `JSON` 格式，`--json` 选项用来解析生成 `JSON` 格式字段。

```bash
mysql> ALTER TABLE `test`.`person` ADD COLUMN `desc` json;

shell> python binlog2sql.py -h127.0.0.1 -P3306 -uadmin -p \
       --start-file='mysql-bin.000003' --start-datetime='2018-12-24 14:30:00'

输出：
INSERT INTO `test`.`person`(`desc`, `id`, `name`) VALUES ('{\"id\": \"3\"}', 3, '你好'); 
#start 2668 end 2927 time 2018-12-24 14:47:13
INSERT INTO `test`.`person`(`desc`, `id`, `name`) VALUES ('[\"你好\", \"世界\"]', 4, '世界'); 
#start 2958 end 3226 time 2018-12-24 15:54:14
```

选项
===

**连接配置**
```
-h host -P port -u user -p password
```

**解析控制**

```
--start-file     起始解析文件。必选，只需文件名而无需全路径。
--stop-file      终止解析文件。可选，默认为start-file同一个文件。若解析模式为stop-never，此选项失效。
--start-position 起始解析位置。可选，默认为start-file的起始位置。
--stop-position  终止解析位置。可选，默认为stop-file的结束位置。若解析模式为stop-never，此选项失效。
--start-datetime 起始解析时间，可选，格式'2018-12-24 14:00:00'，默认不过滤。
--stop-datetime  终止解析时间，可选，格式'2018-12-24 14:30:00'，默认不过滤。
```

**对象过滤**
```
-d, --databases  只解析目标库，多个库用空格隔开，如-d db1 db2。可选。
-t, --tables     只解析目标表，多张表用空格隔开，如-t tbl1 tbl2。可选。
--only-dml       只解析dml，忽略ddl。可选。
--sql-type       支持INSERT, UPDATE, DELETE。多个类型用空格隔开，如--sql-type INSERT DELETE。可选。默认都解析。
```

**其他选项**
```
--stop-never     持续实时解析binlog，直至用户手动 `Ctrl + c` 结束程序。可选。默认False。
--no-primary-key 去除INSERT语句的主键。可选。默认False
--flashback      生成回滚SQL，可解析大文件，不受内存限制。可选。默认False。与stop-never或no-primary-key不能同时添加。
--back-interval  flashback模式下，每打印1000行回滚SQL，线程休眠N秒。可选。默认N=1。
--json           支持JSON格式字段解析。可选，默认不解析JSON字段（如果表中有JSON字段，生成的SQL格式有误）。
```

调试
===
```python
if __name__ == '__main__':
    # args = command_line_args(sys.argv[1:])
    # conn_setting = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password, 'charset': 'utf8'}
    # binlog2sql = Binlog2sql(connection_settings=conn_setting, start_file=args.start_file, start_pos=args.start_pos,
    #                         end_file=args.end_file, end_pos=args.end_pos, start_time=args.start_time,
    #                         stop_time=args.stop_time, only_schemas=args.databases, only_tables=args.tables,
    #                         no_pk=args.no_pk, flashback=args.flashback, stop_never=args.stop_never,
    #                         back_interval=args.back_interval, only_dml=args.only_dml, sql_type=args.sql_type,
    #                         json=args.json)
    conn_setting = {'host': '127.0.0.1', 'port': 3306, 'user': 'root', 'passwd': '123100', 'charset': 'utf8'}
    binlog2sql = Binlog2sql(connection_settings=conn_setting, start_file='mysql-bin.000003', json=True)
    binlog2sql.process_binlog()
```