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

- 本地
```
shell> git clone https://github.com/nloneday/binlog2sql.git && cd binlog2sql
shell> pip install -r requirements.txt
```
- `Docker`镜像
```
shell> docker pull nandy/binlog2sql
```

权限
===

```
mysql> GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO `user`@`%`;

# 说明：
select：需要读取server端information_schema.COLUMNS表，获取表结构的元信息，拼接成可视化的sql语句
replication client：两个权限都可以，需要执行'SHOW MASTER STATUS', 获取server端的binlog列表
replication slave：通过BINLOG_DUMP协议获取binlog内容的权限
```


用法
===

- 最简用法

```
shell> python binlog2sql.py -p

输出：
INSERT INTO `test`.`person`(`id`, `name`) VALUES (1, 'haha'); 
#start 568 end 803 time 2018-12-24 14:04:00
INSERT INTO `test`.`person`(`id`, `name`) VALUES (2, 'wawa'); 
#start 834 end 1069 time 2018-12-24 14:05:23
```

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

- `Docker`镜像
```shell
shell> docker run --rm nandy/binlog2sql /bin/sh -c "python binlog2sql.py -h 192.168.1.10 -P 3306 -u root -p 123100"

输出：
INSERT INTO `test`.`person`(`id`, `name`) VALUES (1, 'haha'); 
#start 568 end 803 time 2018-12-24 14:04:00
INSERT INTO `test`.`person`(`id`, `name`) VALUES (2, 'wawa'); 
#start 834 end 1069 time 2018-12-24 14:05:23

注意：
1. Docker容器模式下，host、password为必填字段，且必须明确写出。
2. 如果使用--output-file选项，必须首先挂载目录（提前创建），如:
   - windows: docker run --rm -v E:\binlog:/tmp nandy/binlog2sql ... --output-file /tmp/backup.sql
   - linux  : docker run --rm -v /home/binlog:/tmp nandy/binlog2sql ... --output-file /tmp/backup.sql
3. 输出文件 E:\binlog\backup.sql、/home/binlog/backup.sql
```

选项
===

**连接选项**
```
-h, --host       可选，MySQL主机。默认为127.0.0.1。
-P, --port       可选，MySQL端口。默认为3306。
-u, --user       可选，MySQL用户。默认为root。
-p, --password   必填，MySQL密码。
```

**解析选项**

```
--start-file     可选，起始解析文件。只需文件名而无需全路径。默认为MySQL当前正在写入的binlog文件。
--stop-file      可选，终止解析文件。默认为start-file同一个文件。若解析模式为stop-never，此选项失效。
--start-position 可选，起始解析位置。默认为start-file的起始位置。
--stop-position  可选，终止解析位置。默认为stop-file的结束位置。若解析模式为stop-never，此选项失效。
--start-time     可选，起始解析时间。格式'yyyy-MM-dd[ hh:mm:ss]'，默认不过滤。
--stop-time      可选，终止解析时间。格式'yyyy-MM-dd[ hh:mm:ss]'，默认不过滤。
```

**过滤选项**
```
-d, --databases  可选，多选，只解析目标库。多个库用空格隔开，如-d db1 db2。
-t, --tables     可选，多选，只解析目标表。多张表用空格隔开，如-t tbl1 tbl2。
--only-dml       可选，只解析dml。忽略ddl。
--sql-type       可选，多选，支持INSERT, UPDATE, DELETE。多个类型用空格隔开，如--sql-type INSERT DELETE。默认都解析。
```

**其他选项**
```
--no-primary-key 可选，去除INSERT语句的主键。默认False
--flashback      可选，生成回滚SQL，可解析大文件。默认False。与stop-never或no-primary-key不能同时添加。
--stop-never     可选，持续实时解析binlog，直至手动 `Ctrl + c` 结束程序。默认False。
--output-file    可选，在打印到屏幕的同时写入本地SQL文件。可选。
--json           可选，支持JSON格式字段解析。默认False，不解析JSON字段（如果表中有JSON字段，生成的SQL格式有误）。
--debug          可选，调试模式。在此模式下不进行任何解析操作，只打印所有的参数和值。
--help           可选，帮助模式。在此模式下不进行任何解析操作，只打印所有帮助信息。
```

调试
===
**`binlog2sql.py`**
```python
if __name__ == '__main__':
    # args = command_line_args(sys.argv[1:])
    # conn_setting = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password}
    # binlog2sql = Binlog2sql(connection_settings=conn_setting, start_file=args.start_file, stop_file=args.stop_file,
    #                         start_position=args.start_position, stop_position=args.stop_position, start_time=args.start_time, stop_time=args.stop_time,
    #                         only_schemas=args.databases, only_tables=args.tables,
    #                         only_dml=args.only_dml, sql_type=args.sql_type,
    #                         no_pk=args.no_pk, flashback=args.flashback, stop_never=args.stop_never, output_file=args.output_file, json=args.json, debug=args.debug)
    # binlog2sql.process_binlog()
    
    conn_setting = {'host': '127.0.0.1', 'port': 3306, 'user': 'root', 'passwd': '123100'}
    binlog2sql = Binlog2sql(connection_settings=conn_setting, json=True)
    binlog2sql.process_binlog()
```
**`binlog2sql_util.py`**
```python
def print_line(line):
    # if pycharm(utf-8)
    print(line)

    # if windows cmd(gbk)
    # if not PY3PLUS and platform.system() == 'Windows':
    #     print(line.decode('utf-8').encode('gbk'))
    # else:
    #     print(line)
```