[![Build Status](https://api.travis-ci.org/qiniu/logkit.svg)](http://travis-ci.org/qiniu/logkit)

logkit
======

Table of Contents
=================

  * [logkit](#logkit)
  * [Production](#production)
  * [Table of Contents](#table-of-contents)
    * [简介](#简介)
    * [服务配置](#服务配置)
  * [Runner配置](#runner配置)
  * [Reader](#reader)
    * [File Reader](#file-reader)
    * [Mysql Reader](#mysql-reader)
    * [Mssql Reader](#mssql-reader)
    * [ElasticSearch Reader](#elasticSearch-reader)
    * [MongoDB Reader](#mongoDB-reader)
  * [Cleaner](#cleaner)
  * [Parser](#parser)
    * [CSV Parser 配置](#csv-parser-配置)
    * [JSON Parser 配置](#json-parser-配置)
    * [Grok Parser 配置](#grok-parser-配置)
    * [Qiniu Log Parser 配置](#qiniu-log-parser-配置)
    * [KafkaRestLog Parser 配置](#kafkarestLog-parser-配置)
    * [Raw Parser 配置](#raw-parser-配置)
  * [Sender](#sender)
    * [File Sender](#file-sender)
    * [Mongodb Accumulate Sender](#mongodb-accumulate-sender)
    * [Pandora Sender](#pandora-sender)
    * [Influxdb Sender](#influxdb-sender)
    * [Elasticsearch Sender](#elasticsearch-sender)
    * [Discard Sender](#discard-sender)
  * [logkit监控](#logkit监控)
  * [自定义Parser和Sender](#自定义Parser和Sender)
  * [带有默认值的Pactice](#带有默认值的Practice)
  * [Best Practice](#best-practice)


简介
------

logkit是七牛推出的开源数据收集、解析、汇聚、发送的工具，带有容错、并发、监控、删除等功能。

文档站： [Pandora Logkit](https://pandora-docs.qiniu.com)

服务配置
------

启动服务的命令中可以指定服务的启动配置

```
go get -u github.com/kardianos/govendor
govendor sync
go build -o logkit logkit.go
./logkit -f logkit.conf
```

logkit.conf 配置如下。

1. `max_procs` go的runtime.GOMAXPROCS
2. `debug_level` 日志输出级别，0为debug，数字越高级别越高
3. `confs_path` 监听Runner配置文件夹，是一个列表，列表中的每一项都是一个监听的配置文件文件夹，如果每一项中文件夹下配置发生增加、减少或者变更，会根据配置创建相应的runner，每个conf文件是一个runner，可以使用表达式来监听文件夹。当符合表达式的文件夹增加或减少时，每隔十秒能检测出变动。
4. `confs_path` 中最大能监听的文件数取决于系统的`fs.inotify.max_user_instances`，请谨慎添加，目前qiniu的服务器默认限制数在128.
5. `clean_self_log` 是否清理logkit本身产生的日志，默认删除 `./run/*.log-*` 匹配命中的日志文件，保留文件名时间戳最新的5个文件。
6. `clean_self_dir` logkit本身日志的路径，默认为 `./run`
7. `clean_self_pattern` logkit本身日志的模式，默认为 `*.log-*`
8. `clean_self_cnt` 保留logkit日志文件个数，默认为 5

```
{
    "max_procs": 8,
    "debug_level": 1,
    "clean_self_log":true,           # 选填，默认false
    "clean_self_dir":"./run",        # 选填，clean_self_log 为true时候生效，默认 "./run" 
    "clean_self_pattern":"*.log-*",  # 选填，clean_self_log 为true时候生效，默认 "*.log-*"
    "clean_self_cnt":5,              # 选填，clean_self_log 为true时候生效，默认 5
    "confs_path": ["confs","confs2", "/home/me/*/confs"]
}
```

注意：清理logkit本身日志的，需要保证日志进行rotate，如果日志只是重定向到一个文件内，那日志是不会被删除的。推荐rotate方式如下：

```
./logkit -f logkit.conf 2>&1 | rotatelogs -l -f ./run/logkit.log-%m%d%H%M%S 500M
```


Runner配置
------

典型的Runner配置如下。

```
{
    "name":"csv_logkit_runner",
    "batch_len": 1000,
    "batch_size": 2097152,
    "batch_interval": 300, 
    "batch_try_times": 3,
    "reader":{
        "log_path":"/home/user/app/log/dir/",
        "meta_path":"./metapath",
        "donefile_retention":"7",
        "read_from":"newest",
        "mode":"dir",
        "ignore_hidden":"true",
        "ignore_file_suffix":".pid, .swp",
        "valid_file_pattern":"qiniulog-*.log" // 可不选，默认为 "*"
    },
     "cleaner":{
        "delete_enable":"true",
        "delete_interval":"10",
        "reserve_file_number":"10",
        "reserve_file_size":"10240"
    },
    "parser":{
        "name":"csv_parser",
        "type":"csv",
        "csv_schema":"logtype string, service string, timestamp long, method string, path string , reqheader string, bucket string, httpcode long, respheader string, unkown1 string, unkown2 long, unkown3 long, unkown4 string, machine string"
    },
    "senders":[{
        "name":"req_io.pandora.sender",
        "sender_type":"pandora",
        "fault_tolerant":"false",
        "pandora_ak":"your_ak",
        "pandora_sk":"your_sk",
        "pandora_host":"https://pipeline.qiniu.com",
        "pandora_repo_name":"repo_req_io",
        "pandora_region":"nb",
        "pandora_schema":"logtype pandora_field1,service,timestamp pandora_field3"
}]
}
```

配置字段说明：

1. `name` 用来标识runner的名字。用来记录日志。
2. `batch_len` 可选，每读取多少行作为一个batch，进行解析和发送。默认无限制
3. `batch_size` 可选，每读取多少数据作为一个batch，单位为byte。默认2097152（2MB:`2*1024*1024`）
4. `batch_interval` 可选，每读取多长时间作为一个batch，无论batch达到多少都直接进行解析和发送。默认60秒
5. `batch_try_times` 可选，每个batch最多尝试发送多少次，如果仍然发送失败，则抛弃该数据。默认永远不抛弃数据始终重试

**注意**

1. `reader` 格式为`map[string]string`的配置，详细配置见`reader`一节
2. `parser` 格式为`map[string]string`的配置，用来配置日志解析方式，详细配置见`parser`一节
3. `senders` 格式为`map[string]string`组成的数组，用来配置日志发送的策略，详细配置见`senders`一节
4. `cleaner`  格式为`map[string]string`组成的数组，用来配置日志的删除策略，详细配置见`cleaner`一节

**警告**

* `batch_size` 这项配置**极端重要**。如果读取单条日志已经超过batch 大小，logkit 会认为这条日志无法发送，**会直接丢弃该日志**。请用户务必重视，在打印日志的时候避免单条日志过长，请避免日志出现大量重复无用的信息。

Reader
======

File Reader
-----

file reader 的典型配置如下

```
    "reader":{
        "log_path":"./logdir",
        "meta_path":"./meta",
        "file_done":"./meta",
        "mode":"dir",
        "encoding":"utf-8",
        "donefile_retention":"7",
        "read_from":"newest",
        "ignore_hidden":"true",  // 可不选，默认开启
        "ignore_file_suffix":".pid, .swp", // 可不选，默认不忽略
        "valid_file_pattern":"qiniulog-*.log" // 可不选，默认为 "*"
    },
```

1. `log_path` 需要收集的日志的文件（夹）路径
2. `meta_path` 是reader的读取offset的记录路径，必须是一个文件夹，在这个文件夹下，会记录本次reader的读取位置
3. `file_done` 是reader完成后标志读取完成的文件放置位置，如果不填，默认放在meta_path下。singleFile模式下，若文件被rotate，则只匹配当前日志文件夹下，前缀相同的文件，inode相同作为rotate后的文件记录到file_done文件中，该策略只是把移动后的第一个文件放到meta的file_done文件里面。
4. `mode` 是读取方式，支持`dir`和`file`两种。当选项为dir的时候，`log_path`必须是文件夹路径，同时会在这个文件夹下根据时间顺序依次读取文件。当选项为`file`的时候，`log_path`必须是文件路径，同时文件会不断的读取最终数据。
5. `read_from` 可选，在创建reader的时候，如果meta信息，即历史读取记录不存在，将从文件的哪一端开始读取。可以设置为`oldest`最旧的部分或者`newest`最新的部分开始读取。如果字段不填，默认从最老的开始消费。
6. `ignore_hidden` 读取的过程中是否忽略隐藏文件，默认忽略
7. `ignore_file_suffix` 读取的过程中忽略哪些文件后缀名，默认不忽略
8. `donefile_retention` 日志读取完毕后donefile的保留时间，默认7天，当然，如果donefile里面的文件一直因为别的runner不能删除，donefile也不会删除。
9. `valid_file_pattern` 需要解析的日志文件名字，方式为glob，默认为全部，即：`*`
10. `encoding` 日志文件的编码方式，默认为`utf-8`，即按照`utf-8`的编码方式读取文件。支持读取文件的编码格式包括：`UTF-16`,`GB18030`,`GBK`,`cp51932`,`windows-51932`,`EUC-JP`,`EUC-KR`,`ISO-2022-JP`,`Shift_JIS`,`TCVN3`及其相关国际化通用别名。


ElasticSearch Reader
-----

ElasticSearch Reader 的典型配置如下

```
    "reader":{
        "es_host":"http://localhost:9200",
        "meta_path":"./meta",
        "mode":"elastic",
        "es_limit_batch":"100",
        "es_index":"app-repo-123",
        "es_type": "type_app",
        "es_keepalive":"1d"
    },
```

1. ElasticSearch Reader 输出的是json字符串，需要使用json的parser解析。
1. `es_host` es的host地址以及端口
1. `meta_path` 是reader的读取offset的记录路径，记录es读取时的ScrollID，路径必须是一个文件夹。
1. `mode` 是读取方式，使用ElasticSearch Reader必须填写`elastic`。
1. `es_limit_batch` es一次获取的batch size，默认为100条一个batch。
1. `es_index` es的索引名称，必填。
1. `es_type` es的type名称
1. `es_keepalive` es reader一旦挂掉了，重启后可以继续读取数据的ID(Offset记录)在es服务端保存的时长，默认1d，写法按es的单位为准，`m`分钟,`h`小时,`d`天。


MongoDB Reader
-----

MongoDB reader 的典型配置如下

```
    "reader":{
        "mongo_host":"localhost:27017",
        "meta_path":"./meta",
        "mode":"mongo",
        "mongo_database":"app123",
        "mongo_collection": "collection1",
        "mongo_offset_key":"_id",
        "mongo_cron":"00 00 04 * * *",
        "mongo_exec_onstart":"true",
        "mongo_filters":"{\"foo\": {\"i\": {\"$gt\": 10}}}",
        "mongo_cacert": "/path/to/cert.pem"
    },
```

1. MongoDB reader 输出的是json字符串，需要使用json的parser解析。
1. `mongo_host` mongo的host地址以及端口
1. `meta_path` 是reader的读取offset的记录路径，记录mongo读取时的Offset，路径必须是一个文件夹。
1. `mode` 是读取方式，使用MongoDB Reader必须填写`mongo`。
1. `mongo_database` mongo的数据库名称
1. `mongo_collection` mongo的表名
1. `mongo_offset_key` 指定一个mongo的列名，作为offset的记录，类型必须是整型(比如unixnano的时间，或者自增的primary key)。
        每次查询会指定这个key做`where条件大于上次查询最后的记录`这样的限制，避免单次查询性能消耗过大，同时也支持持续的数据导入过程中避免数据重复。
        若不指定，则使用mongo的`_id`键，`_id`键是由时间戳(秒)+机器+进程号+自增位组成的，在低频数据写入的情况下是自增的，在多机器组成的mongo集群高并发写入的情况下，`_id`不是自增的，存在漏数据的可能。
        默认使用`_id`，也保证了在纯粹从静态的mongo数据库导入pandora过程中，若重启logkit不会导致数据重复。
1. `mongo_cron` 定时任务触发周期，支持两种写法。
    * crontab的写法,类似于`* * * * * *`，对应的是秒(`0~59`)，分(`0~59`)，时(`0~23`)，日(`1~31`)，月(`1-12`)，星期(`0~6`)，填*号表示所有遍历都执行。
    * 描述式写法,类似于"@midnight", "@every 1h30m"，必须`@`符合开头，目前支持`@hourly`,`@weekly`,`@monthly`,`@yearly`,`@every <time duration>`
1. `mongo_exec_onstart` `true`表示启动时执行一次，以后再按cron处理;`false`则表示到cron预设的时间才执行，默认为true。
1. `mongo_filters` 表示collection的过滤规则，最外层是collection名称，里面对应的是json的规则。如示例所示，表示`foo`这个collection，`i`字段的值大于10的全部数据。
1. `mongo_cacert` 存放mongo的鉴权证书，目前暂时不支持


Mysql Reader
-----

mysql reader是为了让logkit支持多种数据源支持而增加的一种输入模式，区别于普通的文件输入，mysql reader是从mysql中读取数据。

mysql reader是以定时任务的形式去执行mysql语句，将mysql读取到的内容全部获取则任务结束，等到下一个定时任务的到来。

reader 的典型配置如下

```
    "reader":{
        "log_path":"<username>:<password>@tcp(<hostname>:<port>)", // 等价于mysql_datasource
        "meta_path":"./meta",
        "mode":"mysql",
        "mysql_datasource":"<username>:<password>@tcp(<hostname>:<port>)", // 该字段与"log_path"等价，两个都存在的情况下优先取mysql_datasource的值。
        "mysql_database":"<database>",
        "mysql_sql":"select * from xx;select x,y from xx@(YY)",
        "mysql_offset_key":"id",
        "mysql_limit_batch":"100",
        "mysql_cron":"00 00 04 * * *",
        "mysql_exec_onstart":"true"
    },
```

* mysql reader输出的内容必须使用inner sql parser解析
* `mode` : 使用mysql reader，必须模式为mysql
* `mysql_datasource`: 该字段与"log_path"等价，两个都存在的情况下优先取datasource的值。需要按规则填写mysql数据源所需信息。
    - `username`: 用户名
    - `password`: 用户密码
    - `hostname`: mysql地址
    - `port`: mysql端口
    - `示例`：一个填写完整的mysql_datasource字段类似于:"admin:123456@tcp(10.101.111.1:3306)"
* `mysql_cron`: 定时任务触发周期，支持两种写法。
    * crontab的写法,类似于`* * * * * *`，对应的是秒(`0~59`)，分(`0~59`)，时(`0~23`)，日(`1~31`)，月(`1-12`)，星期(`0~6`)，填*号表示所有遍历都执行。
    - 描述式写法,类似于"@midnight", "@every 1h30m"，必须`@`符合开头，目前支持`@hourly`,`@weekly`,`@monthly`,`@yearly`,`@every <time duration>`
* `mysql_database`: 数据库名称。
* `mysql_sql`: 填写要执行的sql语句,可以用@(var)使用魔法变量，用`;`分号隔开，多个语句按顺序执行。
* `mysql_offset_key`: 指定一个mysql的列名，作为offset的记录，类型必须是整型。每次查询会指定这个key做where条件限制，避免单次查询性能消耗过大。
* `mysql_limit_batch`: `mysql_sql`的语句，若数据量大，可以填写该字段，分批次查询。
    - 若`mysql_offset_key`存在，假设填写为100，则查询范式为`select * from table where mysql_offset_key >= 0 and mysql_offset_key < 0 + 100`;
    - 若没填写`mysql_offset_key`，则类似于 `select * from table limit 0,100`。
* `mysql_exec_onstart`: `true`表示启动时执行一次，以后再按cron处理;`false`则表示到cron预设的时间才执行，默认为true。
* `魔法变量`: 目前支持`年`,`月`,`日`,`时`,`分`,`秒`的魔法变量。
    - @(YYYY) 年份
    - @(YY) 年份后两位，如06。
    - @(MM): 月份,补齐两位，如02
    - @(M): 月份，不补齐
    - @(D): 日，不补齐
    - @(DD): 日，补齐两位，如05
    - @(hh): 小时，补齐两位
    - @(h): 小时
    - @(mm): 分钟，补齐两位
    - @(m):  分钟
    - @(ss): 秒，补齐两位
    - @(s): 秒



Mssql Reader
-----

mssql reader是为了让logkit支持多种数据源支持而增加的一种输入模式，区别于普通的文件输入，mssql reader是从 Microsoft SQL Server 中读取数据。

mssql reader是以定时任务的形式去执行sql语句，将sql读取到的内容全部获取则任务结束，等到下一个定时任务的到来。

reader 的典型配置如下

```
    "reader":{
        "log_path":"server=<hostname or instance>;user id=<username>;password=<password>;port=<port>", // 等价于mssql_datasource
        "meta_path":"./meta",
        "mode":"mssql",
        "mssql_datasource":"server=<hostname or instance>;user id=<username>;password=<password>;port=<port>", // 该字段与"log_path"等价，两个都存在的情况下优先取mysql_datasource的值。
        "mssql_database":"<database>",
        "mssql_sql":"select * from xx;select x,y from xx@(YY)",
        "mssql_offset_key":"id",
        "mssql_limit_batch":"100",
        "mssql_cron":"00 00 04 * * *",
        "mssql_exec_onstart":"true"
    },
```

* mssql reader输出的内容必须使用inner sql parser解析
* `mode` : 使用mssql reader，必须模式为mssql
* `mssql_datasource`: 该字段与"log_path"等价，两个都存在的情况下优先取datasource的值。需要按规则填写mssql数据源所需信息。
    - `username`: 用户名
    - `password`: 用户密码
    - `hostname`: mssql地址,实例
    - `port`: mssql端口,默认1433
    - `示例`：一个填写完整的mssql_datasource字段类似于:"server=localhost\\SQLExpress;user id=sa;password=PassWord;port=1433"
* `mssql_cron`: 定时任务触发周期，支持两种写法。
    * crontab的写法,类似于`* * * * * *`，对应的是秒(`0~59`)，分(`0~59`)，时(`0~23`)，日(`1~31`)，月(`1-12`)，星期(`0~6`)，填*号表示所有遍历都执行。
    * 描述式写法,类似于"@midnight", "@every 1h30m"，必须`@`符合开头，目前支持`@hourly`,`@weekly`,`@monthly`,`@yearly`,`@every <time duration>`
* `mssql_database`: 数据库名称。
* `mssql_sql`: 填写要执行的sql语句,可以用@(var)使用魔法变量，用`;`分号隔开，多个语句按顺序执行。
* `mssql_offset_key`: 指定一个mssql的列名，作为offset的记录，类型必须是整型。每次查询会指定这个key做where条件限制，避免单次查询性能消耗过大。
* `mssql_limit_batch`: `mssql_sql`的语句，若数据量大，可以填写该字段，分批次查询。
    - 若`mssql_offset_key`存在，假设填写为100，则查询范式为`select * from table where mssql_offset_key >= 0 and mssql_offset_key < 0 + 100`;
    - 若没填写`mssql_offset_key`，则类似于 `select * from table limit 0,100`。
* `mssql_exec_onstart`: `true`表示启动时执行一次，以后再按cron处理;`false`则表示到cron预设的时间才执行，默认为true。
* `魔法变量`: 目前支持`年`,`月`,`日`,`时`,`分`,`秒`的魔法变量。
    - @(YYYY) 年份
    - @(YY) 年份后两位，如06。
    - @(MM): 月份,补齐两位，如02
    - @(M): 月份，不补齐
    - @(D): 日，不补齐
    - @(DD): 日，补齐两位，如05
    - @(hh): 小时，补齐两位
    - @(h): 小时
    - @(mm): 分钟，补齐两位
    - @(m):  分钟
    - @(ss): 秒，补齐两位
    - @(s): 秒

Cleaner
======

cleaner的典型配置如下

```
     "cleaner":{
        "delete_enable":"true",
        "delete_interval":"10",
        "reserve_file_number":"10",
        "reserve_file_size":"10240"
    },
```

1. `delete_enable` 该选项表示是否启用cleaner，cleaner是删除控制功能，默认不开启，当delete_enable为true时开启。开启后，当reader已经读取完毕的数据，cleaner会负责通知mgr删除。
2. `delete_interval` cleaner执行周期，会在每个周期检查是否符合删除的条件，单位为秒(s)，到了删除周期，且检测发现符合删除条件的数据会被删除，reader读取完毕后会生成file.done文件，当整个file.done文件里的日志都被发送至mgr删除时，文件名会变为file.deleted。
3. `reserve_file_number` 最大保留的已读取文件数，当超过这个数量时就会把多出的文件删除，默认为保留10个.
4. `reserve_file_size` 最大保留已读文件总大小，当已读文件的总大小超过这个值时，会把最老的那部分删掉，默认保留10GB，单位为MB.

*注意*

* cleaner的主要功能是删除的控制，当日志只被一个runner读取时，建议配置cleaner来控制日志的删除。
* 当同一个日志目录有多个runner在读取时，如果不启用cleaner，则表示该runner放弃控制日志的删除。
* 一份日志目录被多个cleaner控制时，只有当所有的cleaner均通知mgr可以删除时，才执行删除日志。


Parser
======

Parser 是用户需要解析自己日志格式的代码。现在仅支持csv格式日志的解析。如果用户需要自己实现解析逻辑，可以参考实现该接口。

```
type LogParser interface {
    Name() string
    // parse lines into structured datas
    Parse(lines []string) (datas []sender.Data, err error)
}
```

Parser 典型配置如下

```
    "parser":{
        "name":"req_csv",
        "type":"csv",
        "csv_schema":"logtype string, service string, timestamp long, method string, path string , reqheader string, bucket string, httpcode long, respheader string, unkown1 string, unkown2 long, unkown3 long, unkown4 string, machine string"
    },
```

1. `name` parser的name，用来标识
2. `type` parser的类型，现在仅支持 csv 格式
3. `csv_schema` csv 的格式定义，详见如下

CSV Parser 配置
-----

CSV Parser 典型配置如下

```
    "parser":{
        "name":"req_csv",
        "type":"csv",
        "csv_schema":"logtype string, service string, timestamp long, method string, path string , reqheader jsonmap, hello jsonmap{a string,b float}, hi jsonmap{a long,...}",
        "labels":"machine nb110,team pandora"
    },
```

* `csv_schema` 是按照逗号分隔的字符串，每个部分格式按照`字段名 字段类型`构成，字段类型现在支持`string`, `long`, `jsonmap`, `float`。
* `labels` 填一些额外的标签信息，同样逗号分隔，每个部分由空格隔开，左边是标签的key，右边是value。
* `csv_splitter` csv文件的分隔符定义，默认为'\t'
* 类型说明
  - `string` go 的string
  - `long` go 的int64
  - `float` go 的float64
  - `jsonmap` 将json反序列化为`map[string]interface{}`，key必须为字符串格式，value为`string`, `long` 或者`float`。如果value不属于这三种格式，将会强制将value转成`string`类型
  - `jsonmap` 如果要指定jsonmap key的类型并且选定一些jsonmap中的key，那么只要用花括号包含选定的key以及其类型即可，里面的语法与外部相同也是以逗号","分隔不同的key和类型。目前不支持嵌套的jsonmap，如果除了选定的key，其他的key也要，就以”...“结尾即可。
* Parser中解析出的字段就是csv_schema中命名的字段，还包括labels中定义的标签名，可以在sender中选择需要发送的字段和标签。


JSON Parser 配置
-----

JSON Parser 典型配置如下

```
    "parser":{
        "name":"req_json",
        "type":"json",
        "labels":"machine nb110,team pandora"
    },
```

* JSON parser是一个schema free的parser，会把json的字符串反序列化成map[string]interface{}，然后交由sender做处理。
* labels中定义的标签如果跟数据有冲突，labels中的标签会被舍弃


Grok Parser 配置
-----

Grok Parser是一个类似于Logstash Grok Parser一样的解析配置方式，其本质是按照正则表达式匹配解析日志。

Grok Parser 典型配置如下

```
    "parser":{
        "name":"nginx_parser",
        "type":"grok",
        "grok_mode":"",#默认为空
        "grok_line_head_pattern":"",#默认为空
        "grok_patterns":"%{QINIU_LOG_FORMAT}",
        "grok_custom_pattern_files":"/etc/logkit/pattern1,/etc/logkit/pattern2",
        "grok_custom_patterns":"",
        "labels":"machine nb110,team pandora"
    },
```

* `grok_patterns` 是按照逗号分隔的字符串，每个部分格式按照`字段名 字段类型`构成，字段类型现在支持`string`, `long`, `jsonmap`, `float`。
* `labels` 填一些额外的标签信息，同样逗号分隔，每个部分由空格隔开，左边是标签的key，右边是value。
* `grok_mode`用来指定解析的方式，留空表示只针对单行进行解析；`multi`表示针对多行就行解析，注意当`grok_mode`为`multi`的时候需要配合`grok_line_head_pattern`使用，读取多行时，最多读取2MB的数据。
* `grok_line_head_pattern`用来在`grok_mode`为多行解析的时候，指定行首的正则表达式，每当匹配到符合行首正则表达式的时候，就将之前的多行一起解析。
* `patterns` 填写解析日志的grok pattern名称，包括一些[logkit自身内置的patterns](https://github.com/qbox/logkit/blob/develop/grok_patterns/logkit-patterns)以及自定义的pattern名称，以及社区的常见grok pattern，如[logstash的内置pattern](https://github.com/logstash-plugins/logstash-patterns-core/blob/master/patterns/grok-patterns)以及常见的[grok库pattern](https://github.com/vjeantet/grok/tree/master/patterns)
    - 填写方式是`%{QINIU_LOG_FORMAT},%{COMMON_LOG_FORMAT}`，以百分号和花括号包裹pattern名称，多个pattern名称以逗号分隔。
    - 实际匹配过程中会按顺序依次去parse文本内容，以第一个匹配的pattern解析文本内容。
    - 需要注意的是，每多一个pattern，解析时都会多解析一个，直到成功为止，所以pattern的数量多有可能降低解析的速度，在数据量大的情况下，建议一个pattern解决数据解析问题。
* `grok_custom_patterns` 用户自定义的grok pattern内容，需符合logkit自定义pattern的写法，**按行分隔**，参见**自定义pattern的写法和用法说明**
* `grok_custom_pattern_files` 用户自定义的一些grok pattern文件，当自定义pattern太长太多，建议用文件功能。
* `Grok Parser中解析出的字段` 就是grok表达式中命名的字段，还包括labels中定义的标签名，可以在sender中选择需要发送的字段和标签。
* `logkit grok pattern` 其格式符合 `%{<捕获语法>[:<字段名>][:<字段类型>]}`，其中中括号的内容可以省略
    - logkit的grok pattern是logstash grok pattern的增强版，除了完全兼容[logstash grok pattern规则](https://www.elastic.co/guide/en/logstash/current/plugins-filters-grok.html#_grok_basics)以外，还增加了类型，与[telegraf的grok pattern规则](https://github.com/influxdata/telegraf/tree/master/plugins/inputs/logparser#grok-parser)一致，但是使用的类型是logkit自身定义的。你可以在[logstash grok文档](https://www.elastic.co/guide/en/logstash/current/plugins-filters-grok.html)中找到详细的grok介绍.
    - `捕获语法` 是一个正则表达式的名字，比如内置了`USERNAME [a-zA-Z0-9._-]+`,那么此时`USERNAME`就是一个捕获语法。所以，在使用自定义的正则表达式之前，你需要先为你的正则命名，然后把这个名字当作`捕获语法`填写`patterns`中，当然，不推荐自己写正则，建议首选内置的捕获语法。
    - `字段名` 按照捕获语法对应的正则表达式解析出的字段，其字段名称以此命名，该项可以不填，但是没有字段名的grok pattern不解析，无法被logkit sender使用，但可以作为一个中间`捕获语法`与别的`捕获语法`共同组合一个新的`捕获语法`。
    - `字段类型` 可以不填，默认为string。logkit支持以下类型。
       * `string` 默认的类型
       * `long` 整型
       * `float` 浮点型
       * `date` 时间类型，包括以下格式
       	 - `2006/01/02 15:04:05`,
		 - `2006-01-02 15:04:05 -0700 MST`,
		 - `2006-01-02 15:04:05 -0700`,
		 - `2006-01-02 15:04:05`,
		 - `2006/01/02 15:04:05 -0700 MST`,
		 - `2006/01/02 15:04:05 -0700`,
		 - `2006-01-02 -0700 MST`,
		 - `2006-01-02 -0700`,
		 - `2006-01-02`,
		 - `2006/01/02 -0700 MST`,
		 - `2006/01/02 -0700`,
		 - `2006/01/02`,
		 - `Mon Jan _2 15:04:05 2006` ANSIC,
		 - `Mon Jan _2 15:04:05 MST 2006` UnixDate,
		 - `Mon Jan 02 15:04:05 -0700 2006` RubyDate,
		 - `02 Jan 06 15:04 MST` RFC822,
		 - `02 Jan 06 15:04 -0700` RFC822Z,
		 - `Monday, 02-Jan-06 15:04:05 MST` RFC850,
		 - `Mon, 02 Jan 2006 15:04:05 MST` RFC1123,
		 - `Mon, 02 Jan 2006 15:04:05 -0700` RFC1123Z,
		 - `2006-01-02T15:04:05Z07:00` RFC3339,
		 - `2006-01-02T15:04:05.999999999Z07:00` RFC3339Nano,
		 - `3:04PM` Kitchen,
		 - `Jan _2 15:04:05` Stamp,
		 - `Jan _2 15:04:05.000` StampMilli,
		 - `Jan _2 15:04:05.000000` StampMicro,
		 - `Jan _2 15:04:05.000000000` StampNano,
       * `drop` 表示扔掉该字段
    - `验证自定义pattern的正确性`：[http://grokdebug.herokuapp.com](http://grokdebug.herokuapp.com), 这个网站可以debug你的grok pattern


Inner SQL Parser 配置
----

选择了mysql 或者 mssql作为数据源后，必须选择inner sql parser

Inner SQL Parser 典型配置如下

```
    "parser":{
        "name":"innermql",
        "type":"_sql",
        "labels":"machine nb110,team pandora",
        "inner_sql_schema":"field1 long,field2 float,field3 date,field"
    },
```

* `type` 必须填写 "_sql"
* `labels` 填一些额外的标签信息，同样逗号分隔，每个部分由空格隔开，左边是标签的key，右边是value。
* `inner_sql_schema` 按顺序填写sql那边传来的数组对应的字段名和类型，顺序不能错，类型不能填错,仅限`long`,`string`,`date`,`float`，类型不写则默认是`string`。sql读到的都是string, parser这边进行类型转换。


Qiniu Log Parser 配置
-----

Qiniu Log Parser解析的是 `github.com/qiniu/log` 生成的app日志。

Qiniu Log Parser 典型配置如下

```
    "parser":{
        "name":"pandora_qiniulog",
        "type":"qiniulog",
        "qiniulog_prefix":"pandora",
        "qiniulog_max_line":1000,
        "labels":"machine nb110,team pandora"
    },
```

* `qiniulog_prefix` 是使用github.com/qiniu/log这个库时用到的前缀，若没用上，就不填，通常情况下没有配置，默认不填。
* `labels` 填一些额外的标签信息，同样逗号分隔，每个部分由空格隔开，左边是标签的key，右边是value。
* `qiniulog_max_line` 是为了防止一旦出现超长错误日志导致batch过长发不出去阻塞队列，默认最长1000行，建议不要配置过长。
* `qiniulog_log_headers`可以指定字段名称的顺序， 默认为`prefix`、`date`、`time`、`reqid`、`level`、`file`
  - `prefix` qiniulog的前缀，默认为空，不解析。
  - `date` 日志容器
  - `time` 日志时间
  - `reqid` 日志中用户请求的ID
  - `level` 日志等级
  - `file` 日志产生的代码位置
  - 例如teapot的日志，可以配置成: `prefix,date,time,level,reqid,file,log`
* `log` 字段表示日志体的内容，顺序一定是在最后，不能改变。
* 最终qiniulog parser解析出来的字段为`prefix`、`date`、`time`、`reqid`、`level`、`file`、`log`以及标签，可以在sender中选择需要发送的字段和标签。

KafkaRestLog Parser 配置
-----

KafkaRestLog Parser将日志文件的每一行解析为一条日志.

KafkaRestLog Parser 典型配置如下:

```
    "parser":{
        "name":"pandora_kafkarest_log",
        "type":"kafkarest",
        "labels":"machine nb123, platform kirk, team pandora"
    },
```

* `labels` 填一些额外的标签信息，同样逗号分隔，每个部分由空格隔开，左边是标签的key，右边是value。
* KafkaRestLog 解析出的字段名是固定的，包括如下字段及标签，可以在sender中选择需要发送的字段和标签。
  - `source_ip`: 源IP
  - `method`: 请求的方法，诸如POST、GET、PUT、DELETE等
  - `topic`: 请求涉及的kafka topic
  - `code`: httpcode
  - `resp_len`: 请求长度
  - `duration`: 请求时长
  - `log_time`: 日志产生的时间
  - `error`: 表示是一条error日志，也只有error日志才解析出该字段
  - `warn`: 表示是一条warn日志，也只有warn日志才解析出该字段



Raw Parser 配置
-----

Raw Parser将日志文件的每一行解析为一条日志，解析后的日志由两个字段，`raw`和`timestamp`，前者是日志，后者为解析该条日志的时间戳。

Raw Parser 典型配置如下

```
    "parser":{
        "name":"pandora_raw",
        "type":"raw",
        "labels":"machine nb110,team pandora"
    },
```

* `labels` 填一些额外的标签信息，同样逗号分隔，每个部分由空格隔开，左边是标签的key，右边是value。
* Raw Parser 解析出的字段名是固定的，包括如下字段及标签，可以在sender中选择需要发送的字段和标签。
  - `raw`: 每一行的具体内容，若为空行，则忽略
  - `timestamp`: 时间戳

Sender
=====

Sender 的典型配置如下

```
"senders":[{
        "name":"pandora_sender",
        "sender_type":"pandora",
        "pandora_ak":"",
        "pandora_sk":"",
        "pandora_host":"https://pipeline.qiniu.com",
        "pandora_repo_name":"yourRepoName",
        "pandora_region":"nb",
        "pandora_schema":"field1 pandora_field1,field2,field3 pandora_field3",
        "fault_tolerant":"true",
        "ft_sync_every":"5",
        "ft_save_log_path":"./ft_log",
        "ft_write_limit":"1",
        "ft_strategy":"always_save",
        "ft_procs":"2"
}]
```

1. `name`： 是sender的标识
2. `sender_type`： sender类型，支持`file`, `mongodb_acc`, `pandora`, `influxdb`
3. `fault_tolerant`： 是否用异步容错方式进行发送，默认为false。
4. `ft_save_log_path`: 当`fault_tolerant`为true时候必填。该路径必须为文件夹，该文件夹会作为本地磁盘队列，存放数据，进行异步容错发送。
5. `ft_sync_every`：当`fault_tolerant`为true时候必填。多少次发送数据会记录一次本地磁盘队列的offset。
6. `ft_write_limit`：选填，为了避免速率太快导致磁盘压力加大，可以根据系统情况自行限定写入本地磁盘的速率，单位MB/s。默认10MB/s
7. `ft_strategy`： 选填，该选项设置为`backup_only`的时候，数据**不经过**本地队列直接发送到下游，设为`always_save`时则所有数据会先发送到本地队列。无论该选项设置什么，失败的数据都会加入到重试队列中异步循环重试。默认选项为`always_save`。
8. `ft_procs` ：该选项表示从本地队列获取数据点并向下游发送的并发数，如果ft_strategy设置为`backup_only`，则本项设置无效，只有本地队列有数据时，该项配置才有效，默认并发数为1.

补充说明

* 设置`fault_tolerant`为"true"时,会维持一个本地队列缓存起需要发送的数据。当数据发送失败的时候会在本地队列进行重试，此时如果发送错误，不会影响logkit继续收集日志。
* 设置`fault_tolerant`为"true"时,可以保证每次服务重启的时候都从上次的发送offset继续进行发送。在parse过程中产生中间结果，需要高容错性发送的时候最适合采用此种模式。
* 设置`fault_tolerant`为"true"时,一般希望日志收集程序对机器性能影响较小的时候，建议首先考虑将`ft_strategy`设置为`backup_only`，配置这个选项会首先尝试发送数据，发送失败的数据才放到备份队列等待下次重试。如果还希望更小的性能影响，并且数据敏感性不高，也可以不使用`fault_tolerant`模式。
* 当日志发送的速度已经赶不上日志生产速度时，设置`fault_tolerant`为"true"，且`ft_strategy`设置为`always_save`，通过设置`ft_procs`加大并发，`ft_procs`设置越大并发度越高，发送越快，对机器性能带来的影响也越大。
* 如果`ft_procs`增加已经不能再加大发送日志速度，那么就需要 加大`ft_write_limit`限制，为logkit 的队列提升磁盘的读写速度。
* senders支持多个sender配置，但是我们不推荐在senders中加入多个sender，因为一旦某个sender发送缓慢，就会导致其他sender等待这个sender发送成功后再发。简单来说，配置多个sender会互相影响。

File Sender
-----

File Sender 典型配置：

```
{
        "name":"file_sender",
        "sender_type":"flie",
        "fault_tolerant":"false",
        "file_send_path":"./a.txt"
}
```

`file_send_path` 发送文件路径，会将数据按照行，使用json格式写入本地文件。（推荐在做测试时使用该模式）


Mongodb Accumulate Sender
-----

Mongodb Accumulate 典型配置

```
{
        "name":"mongodb_acc_sender",
        "sender_type":"mongodb_acc",
        "mongodb_host":"127.0.0.1:27017",
        "mongodb_db":"billing",
        "mongodb_collection":"req_io_5m",
        "mongodb_acc_updkey":"domain,uid,bucket,time5Min time,zone,idc,src",
        "mongodb_acc_acckey":"flow,hits",
        "fault_tolerant":"false"
}
```

1. `mongodb_host` Mongodb 的地址
2. `mongodb_db` Mongodb 的数据库
3. `mongodb_collection` Mongodb 的Collection
4. `mongodb_acc_updkey` Mongodb 的聚合条件列，按逗号分隔各列名。如果需要在写入mongodb时候对列名进行重命名，配置时候只要在原列名后增加新名字即可，如`time5Min time`将parse 结果中的time5Min，写入mongodb的time字段。
5. `mongodb_acc_acckey` Mongodb 的聚合列，按照逗号分隔各列名。如果需要在写入mongodb时候对列名进行重命名，配置时候只要在原列名后增加新名字即可，用法同`mongodb_acc_updkey`。

Pandora Sender
-----

Pandora 典型配置

```
{
        "name":"req_io.pandora.sender",
        "sender_type":"pandora",
        "fault_tolerant":"false",
        "pandora_ak":"",  #可以直接填写ak，也可使用系统环境变量："pandora_ak":"${YOUR_ENV_AK}"
        "pandora_sk":"", #可以直接填写sk，也可使用系统环境变量："pandora_sk":"${YOUR_ENV_SK}"
        "pandora_host":"https://pipeline.qiniu.com",
        "pandora_repo_name":"repo_req_io",
        "pandora_region":"nb",
        "pandora_schema":"field1 pandora_field1,field2,field3 pandora_field3,...",
        "pandora_auto_create":"pandora_field1 *s,field2 l,pandora_field3 d,field4 f",
        "pandora_schema_update_interval":"300"
}
```

1. `pandora_ak` qiniu账号的ak.可以直接填写ak，也可使用`"${YOUR\_ENV\_AK\_NAME}"`从系统环境变量`YOUR_ENV_AK_NAME`中读取ak
1. `pandora_sk` qiniu账号的sk.可以直接填写sk，也可使用`"${YOUR\_ENV\_SK\_NAME}"`从系统环境变量`YOUR_ENV_SK_NAME`中读取sk
1. `pandora_host` pandora 服务地址
1. `pandora_repo_name` pandora 的repo名字
1. `pandora_region` pandora 服务的地域
1. `pandora_schema` 是可选字段，提供了schema的选项和别名功能，如果不填，则认为所有parser出来的field只要符合pandora schema就发送；如果填，可以只选择填了的字段打向pandora，根据逗号分隔，如果要以别名的方式打向pandora，加上空格跟上别名即可。若最后以逗号省略号",..."结尾则表示其他字段也以pandora的schema为准发送。
1. Pandora Sender会在runner启动时获取repo的schema，若用户缺少一些必填的schema，会帮助填上默认值。
1. `pandora_schema_update_interval` 是自动更新pandora schema的周期时间，单位为秒，默认300s
1. `pandora_auto_create` 该字段表示字段创建，默认为空，不自动创建。若填写该字段，则表示自动创建。repo的DSL创建规则为`<字段名称> <类型>`,字段名称和类型用空格符隔开，不同自动用逗号隔开。若字段必填，则在类型前加`*`号表示。
    * pandora date类型：`date`,`DATE`,`d`,`D`
    * pandora long类型：`long`,`LONG`,`l`,`L`
    * pandora float类型: `float`,`FLOAT`,`F`,`f`
    * pandora string类型: `string`,`STRING`,`S`,`s`
    * pandora bool类型:  `bool`,`BOOL`,`B`,`b`,`boolean`
    * pandora array类型: `array`,`ARRAY`,`A`,`a`;括号中跟具体array元素的类型，如a(l)，表示array里面都是long。同时，也可以省略小括号前的array类型申明，直接写`(l)`，表示 array类型，里面的元素是long
    * pandora map类型: `map`,`MAP`,`M`,`m`;使用花括号表示具体类型，表达map里面的元素，如map{a l,b map{c b,x s}}, 表示map结构体里包含a字段，类型是long，b字段又是一个map，里面包含c字段，类型是bool，还包含x字段，类型是string。同时，也可以省略花括号前面的map类型，直接写`{s l}`,表示map类型，里面的元素s为long类型。




Influxdb Sender
-----

Influxdb 典型配置

```
{
        "name":"influxdb_sender",
        "sender_type":"inlfuxdb",
        "fault_tolerant":"false",
        "influxdb_host":"127.0.0.1:8086",
        "influxdb_retention":"oneDay",
        "influxdb_db":"testdb",
        "influxdb_measurement":"test_table",
        "influxdb_tags":"bucket, service",
        "influxdb_fields":"timestamp, logtype, service, method, path",
        "influxdb_timestamp":"timestamp",
        "influxdb_timestamp_precision":"100"
}
```

1. `influxdb_host` influxdb 服务地址
2. `influxdb_db` influxdb 的数据库名
2. `influxdb_retention` influxdb 的retention policy名字，可不填
3. `influxdb_measurement` influxdb 的measurement名字
4. `influxdb_tags` influxdb 的tag 列名,用","逗号分隔，分隔后每一个字符串中间有空格，则认为是起了别名，如"name alias,name2"这样
5. `influxdb_fields` influxdb 的field 列名，同tags
6. `influxdb_timestamp` influxdb 的时间戳列名
7. `influxdb_timestamp_precision` 时间戳列的精度，如果设置100，那么就会在send的时候将`influxdb_timestamp*100`作为nano时间戳发送到influxdb


Discard Sender
-----

Discard Sender 典型配置

```
{
        "name":"discard_sender",
        "sender_type":"discard"
}
```

* discard sender 不发送给任何一个sender，但是为了使用cleaner的功能，将整个逻辑走完。纯粹使用logkit进行日志的删除。



Elasticsearch Sender
-----


Elasticsearch 典型配置

```
{
        "name":"influxdb_sender",
        "sender_type":"elasticsearch",
        "elastic_host":"localhost:9200",
        "elastic_index":"test",
        "elastic_type":"testType",
        "elastic_keys":"oldKey newKey,oldKey2 newKey2"
}
```

1. `elasitc_host` elasticsearch 服务地址.多个地址使用`,`分隔
1. `elastic_index` elasticsearch 的索引名
1. `elastic_type` elasticsearch 索引下的type，默认为`logkit`
1. `elastic_keys` key 名字.用","逗号分隔，分隔后每一个字符串中间有空格，则认为是起了别名，如"name alias,name2"这样


自定义Parser和Sender
------

logkit不仅包含开箱即用的功能，同时支持用户根据自己的业务场景进行定制化开发。

对于自定义parser

用户只需要实现LogParser接口，以及实现一个该类型的构造函数：

```
type LogParser interface {
	Name() string
	Parse(lines []string) (datas []sender.Data, err error) // parse lines into structured datas
}


func NewMyParser(c conf.MapConf) (parser.LogParser, error) {
    // TODO implement your constructor
}
```

对于自定义sender

用户只需要实验Sender接口

```
type Sender interface {
	Name() string
	Send([]Data) error  // send data, error if failed
	Close() error
}

func NewMySender(c conf.MapConf) (sender.Sender, error) {
    // TODO implement your constructor
}
```


在启动的时候注册好自己的parser，并将其注入到Manager中

```
pregistry := parser.NewParserRegistry()
// 注册自定义parser
pregistry.RegisterParser("myparser", samples.NewMyParser)

sregistry := sender.NewSenderRegistry()
// 注册自定义sender
sregistry.RegisterSender("mysender", samples.NewMySender)

m, err := mgr.NewCustomManager(conf.ManagerConfig, pregistry, sregistry)
```

具体的示例可以参见代码中的samples 模块，该模块实现了一个简单的parser。剩下的用法就跟之前的logkit完全一样了。在你的parser中配置你的自定义parser即可。
注意，在runner配置里面，不仅仅可以使用你自己自定义的parser，sender，同样可以使用logkit自带的parser和sender。

```
"parser":{
    "name":"my_simple_parser",
    "type":"myparser",
    "max_len":"20"
},
"senders":[{
    "name":"mysender",
    "sender_type":"mysender",
    "fault_tolerant":"true",
    "ft_save_log_path":"./samples/ft",
    "ft_sync_every":"2000",
    "ft_write_limit":"10",
    "file_send_path":"./export_meta_test_csv_file.txt"
}]
```


logkit监控
------

logkit提供http API进行整体的监控，启动logkit后，会从4000开始寻找机器上可用的端口，启动成功后在logkit运行的目录下生成一个名为`stats`shell文件，包含端口信息和curl API的shell语句，执行

```
./stats
```

就可以获得logkit整体的运行状态(json格式的信息)。API及返回内容说明如下:

```
GET /logkit/stats
```

```
200 OK
{
   "runner名字": {
        "name" : <runner名字>,
        "logpath": <日志目录>,
        "lag": {
            "size": <延迟的日志总量>,
            "files": <延迟的文件数>,
            "ftlags": <fault_tolerant 队列深度>
        },
        "parserStats": {
            "errors": <解析失败总次数>,
            ”success“: <解析成功总次数>
        },
        ”senderStats“:{
          "<senderName>":  {
                "errors":<发送失败总次数>,
                "success":<发送成功总次数>
            }
        },
        "error":<错误信息>
    }
}
```

* 出现延迟（lag），则表示解析或者发送过于缓慢，可以调整发送方式，使用`fault_tolerant` sender，设置`always_save`，并调大`ft_procs`，参见[Sender](https://github.com/qbox/logkit#sender)一节
* `ftlags` 表示已经使用了`fault_tolerant`，但是由于sender并发不够多或者发送端服务故障，导致出现延迟，`ftlags`的单位为batch数。
* `parserStats`中包含的errors是解析失败的次数，解释失败后该记录会被忽略(不会重试)，错误的详细信息会在logkit日志中打印。
* `senderStats`中包含的errors为发送失败的次数，发送失败后会重新发送，所以sender的错误会多次出现。
* `error` 包含的是调用接口时，某个runner获取信息失败时的错误原因

补充说明：

对于将logkit作为第三方库，自定义实现logkit功能的用户，如果需要开启rest服务，提供监控，需要在自主的主程序（main函数）中加入rest服务的启动过程。

```
rs := mgr.NewRestService(m)
```

同时在主程序结束时关闭rest服务

```
rs.Stop()
```


带有默认值的Practice
------

如下为一份配置，“#”号标注指带有默认值

```
{
    "name":"exportd.csv",
    #"batch_len": 1000,
    #"batch_size": 2097152,
    "reader":{
        "log_path":"/path/to/log",
        #"meta_path":"./meta/{{.env}}/reader", # defalut: CURRENT_WORK_DIR/meta/<NAME-{hash(NAME)}>/
        #"mode":"dir",
        #"ignore_hidden":"true",
        #"valid_file_pattern":"*"
    },
   	 #default not using cleaner
    "cleaner":{
        "delete_enable":"true", # if enable, there are serveral defalut values
        "delete_interval":"10",  # defalut:300s
        "reserve_file_number":"5", # defalut:10
        "reserve_file_size":"5120" # defalut:2048MB
    },
    "parser":{
        "name":"exportd_csv",
        "type":"csv",
	    #"csv_splitter":"\t",
        "csv_schema":"logtype string, service string, timestamp long, tags jsonmap, fields jsonmap,unkown string",
        "labels":"machine {{.node}}"
    },
    "senders":[{
        "name":"pandora_sender",
        "sender_type":"pandora",
        "pandora_ak":"<your qiniu access key>",
        "pandora_sk":"<your qiniu secret key>",
        "pandora_host":"https://pipeline.qiniu.com",
        "pandora_repo_name":"<your pandora repo(工作流名称)>",
        "pandora_region":"nb",
	    "pandora_auto_create":"member_id l,video_id l,video_name s,game_id l,game_game s"
    }]
}
```



Best Practice
------

在实际使用logkit的过程中，我们总结了一些最佳实践：

1. 如果有多个sender，并且希望sender之间互不影响，那么我们建议做成多份配置文件，而不是单份配置文件写多个sender。
1. 如果多个reader，并且每个reader都不能遗漏日志，那么每一个reader都应该对应配置一个cleaner，来控制日志的删除。
1. 在发送到Pandora的过程中，如果发现repo创建错误，需要修改，那么先把发送到pandora的logkit配置移除，修改为新的以后，再将配置移入，无需重启logkit。
1. 我们在grok_patterns里面写了一些基本的示例，其中NGINX_LOG比COMMON_LOG_FORMAT性能好，PADNORA_NGINX是我们自己的nginx配置生成的匹配，更全面。grok的pattern编写完请一定要看一下benchmark，参阅[grok文章](https://www.elastic.co/blog/do-you-grok-grok)


Enjoy it！
