# Cluster

logkit cluster

## 架构

1. master <-> slave 结构，slave向所有master定时发送心跳(注册)
2. master可以有多个，master无状态
3. 通过tag标签来区分不同的slave，以执行不同的任务，以便从逻辑上对所有slave进行区分

## 主要功能

* 访问master可以查看所有logkit的运行状态
* 通过master可以部署所有logkit（或指定某个logkit）执行日志收集任务（增、删、改）

## API

### Master API -- 注册slave

POST /logkit/cluster/register
{
  "url":"slave_url",
  "tag":"first"
}

###  Master API -- 获取slave列表

GET /logkit/cluster/slaves?tag=tagvalue

返回

[{"url":"http://10.10.0.1:1222","tag":"tag1","status":"ok","last_touch":<rfc3339 string>}]

* status 状态有三种， ok，表示正常，30s内有联系；bad，表示1分钟内有联系；lost，表示超过1分钟无心跳
* 可以通过tag url参数获取一类tag的列表


###  Master API -- 获取runner状态列表

GET /logkit/cluster/status?tag=tagvalue

返回

```
Content-Type: application/json

{
 "url1":{
    "status":{
       "runner1": {
          "name":"runner1",
          "logpath":"/your/log/path1",
          "readDataSize": <读取数据的bytes大小>.
          "readDataCount":<读取数据条数>,
          "elaspedtime":<总用时>,
          ... //此处省略，与get status
       }
    },
    "tag":<tag>,
    "error":<error info>
  }
  ...
}

```


### Slave API -- 注册（修改）标签

POST /logkit/cluster/tag
{
  "tag":"first"
}

修改slave 的标签，slave向master发送心跳时通过register接口顺便做修改