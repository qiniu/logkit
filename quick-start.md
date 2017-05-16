logkit
======

简介
------

logkit是Pandora开发的一个通用的日志收集工具，可以将不同数据源的数据方便的发送到Pandora进行数据分析，除了基本的数据发送功能，logkit还有容错、并发、监控、删除等功能。

支持的数据源
------

1. 文件(包括csv格式的文件，kafka-rest日志文件，nginx日志文件等,并支持以[grok](https://www.elastic.co/blog/do-you-grok-grok)的方式解析日志)
2. MySQL
3. Microsoft SQL Server(MSSQL)
4. elastic search
5. mongodb

工作方式
-----
logkit本身支持多种数据源，并且可以同时发送多个数据源的数据到Pandora，每个数据源对应一个逻辑上的runner，一个runner负责一个数据源的数据推送，工作原理如下图所示


![logkit 工作原理图](http://op26gaeek.bkt.clouddn.com/logkit%20arch.png)

使用方式
------

1. 下载&解压logkit工具
```
wget http://op26gaeek.bkt.clouddn.com/logkit.tar.gz && tar xvf logkit.tar.gz
```

2. 修改runner的配置
```
打开 _package/confs/default.conf
```
按照图示进行修改
![需要修改的字段](http://op26gaeek.bkt.clouddn.com/logkit%20conf.png)

3. 启动logkit工具
```
cd _package && ./logkit -f logkit.conf
```

logkit.conf是logkit工具本身的配置文件，主要用于指定logkit运行时需要的资源和各个runner配置文件的具体路径。

1. `max_procs` logkit运行过程中最多用到多少个核
2. `debug_level` 日志输出级别，0为debug，数字越高级别越高
3. `confs_path` 是一个列表，列表中的每一项都是一个runner的配置文件夹，如果每一项中文件夹下配置发生增加、减少或者变更，logkit会相应的增加、减少或者变更runner，配置文件夹中的每个配置文件都代表了一个runner。

典型的配置如下：
```
{
    "max_procs": 8,
    "debug_level": 1,
    "confs_path": ["confs"]
}
```
上面的配置指定了一个runner的配置文件夹，这个配置文件夹下面每个以.conf结尾的文件就代表了一个运行的runner，也就代表了一个logkit正在运行的推送数据的线程。


配置详解
------

典型的 csv Runner配置如下。

```
{
    "name":"csv_runner", # 用来标识runner的名字,用以在logkit中区分不同runner的日志
    "reader":{
        "mode":"dir", # 是读取方式，支持`dir`和`file`两种
        "log_path":"/home/user/app/log/dir/", # 需要收集的日志的文件（夹）路径
        "meta_path":"./metapath", # 是reader的读取offset的记录路径，必须是文件夹
    },
    "parser":{
        "name":"csv_parser", # parser的名字，用以在logkit中区分不同的parser
        "type":"csv",
        "csv_schema":"timestamp long, method string, path string, httpcode long" # 按照逗号分隔的字符串，每个部分格式按照`字段名 字段类型`构成，字段类型现在支持`string`, `long`, `jsonmap`, `float`
    },
    "senders":[{ # senders是
        "name":"pandora_sender",
        "sender_type":"pandora", # 如果数据要发送到Pandora，那么必须写pandora
        "pandora_ak":"your_ak", # 账号的ak
        "pandora_sk":"your_sk", # 账号的sk
        "pandora_host":"https://pipeline.qiniu.com",
        "pandora_repo_name":"your_repo_name", # 账号的repo name
        "pandora_region":"nb",
        "pandora_schema":"" # 留空表示将parse出来的字段全数发到pandora
}]
}
```

对于csv文件的上传，只需要修改上述配置文件的`log_path`,`csv_schema`,`pandora_ak`,`pandora_sk`,`pandora_repo_name`就完成了一个基本的上传csv文件的logkit配置。

** 对于需要更多数据源类型的使用者，可以接着阅读[更多内容](https://github.com/qbox/logkit/blob/develop/README.md)。**
