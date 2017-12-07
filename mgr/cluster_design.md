# Cluster

logkit cluster

## 架构

1. master <-> slave 结构，slave向所有master定时发送心跳(注册)
2. master可以有多个，master无状态
3. 通过tag标签来区分不同的slave，以执行不同的任务，以便从逻辑上对所有slave进行区分

## 主要功能

* 访问master可以查看所有logkit的运行状态
* 通过master可以部署所有logkit（或指定某个logkit）执行日志收集任务（增、删、改）

## 配置

### 示例配置
logkit 的 cluster 功能配置非常简单，只需要在 logkit 的主配置文件中添加下面的配置即可

对于 master 来说, 应该添加:
```
{
    "cluster": {
        "master_url": [],
        "is_master": true,
        "enable": true
    }
}
```
对于 slave 来说，假设其 master 的 url 为`192.168.0.2:3000`, 应该添加:
```
{
    "cluster": {
        "master_url": ["192.168.0.2:3000"],
        "is_master": false,
        "enable": true
    }
}
```
以 slave 为例，完整的 logkit 主配置文件示例如下：
```
{
    "max_procs": 8,                  # 选填，默认为机器的CPU数量
    "debug_level": 1,                # 选填，默认为0，打印DEBUG日志
    "bind_host":"127.0.0.1:4000",    # 选填，默认自己找一个4000以上的可用端口开启
    "profile_host":"localhost:6060", # 选填，默认为空，不开启
    "clean_self_log":true,           # 选填，默认false
    "clean_self_dir":"./run",        # 选填，clean_self_log 为true时候生效，默认 "./run"
    "clean_self_pattern":"*.log-*",  # 选填，clean_self_log 为true时候生效，默认 "*.log-*"
    "clean_self_cnt":5,              # 选填，clean_self_log 为true时候生效，默认 5
    "rest_dir":"./.logkitconfs",     # 选填，通过web页面存放的logkit配置文件夹，默认为logkit程序运行目录的子目录`.logkitconfs`下
    "static_root_path":"./public",   # 必填，logkit页面的静态资源路径，即项目中public目录下的内容，包括html、css以及js文件，请尽量填写绝对路径
    "timeformat_layouts":["[02/Jan/2006:15:04:05 -0700]"], #选填，默认为空。
    "confs_path": ["confs","confs2", "/home/me/*/confs"], #必填，监听的日志目录
    "cluster": {                                          # 启用 cluster 功能时填写.
        "master_url": ["192.168.0.2:3000"],
        "is_master": false,
        "enable": true
    }
}
```
### 参数说明

此处仅介绍 cluster 配置的字段信息，完整的 logkit 主配置文件字段说明请参考 [logkit主配置文件](https://github.com/qiniu/logkit/wiki/logkit%E4%B8%BB%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6)

cluster 配置参数说明如下:

|参数名称|参数类型|是否必填|参数说明|
|:---:|:---:|:---:|:---|
|master_url|string 数组|slave 必填<br/>master 选填|`master_url`中的每一项都应该是一个url(包括端口号)，它们是当前 logkit 各个 master 的 url<br/>1. 对于 slave, 它会定期向每个链接发心跳注册，以便让其 master 获取自己的状态<br/>2. 对于 master, 当填写该字段后，它本身也会作为 slave 受到它的 master 控制，当然这个 master 可以是它自己。|
|is_master|bool|必填|标明当前 logkit 是否是 master:<br/>1. master 请置为 true<br/>2. slave 请置为 false|
|enable|bool|必填|是否启用 cluster 功能， master 和 slave 都应该置为 true|

注意：

* 在启用 cluster 功能时，建议将 logkit 主配置文件中的 `bind_host` 字段显式绑定一个可以保证`master`、`slave`能够互相通信的`ip地址`和`端口`。 因为当此处的`ip地址`为空时，作为`slave`的`logkit`会自行获取本机的一个`ip地址`，并在向`master`发心跳时将该`ip地址`发送给`master`, `master`会利用该`ip`与`slave`进行通信。此时，若该`ip`与`master`不在同一网段，就会造成`master`无法访问`slave`的情况。
* 在配置结束后，建议先启动作为`master`的`logkit`，再启动作为`slave`的`logkit`，这样可以避免`slave`产生向`master`发心跳失败的错误日志。

## API
### Master API -- 是否为 Master
```
GET /logkit/cluster/ismaster
```
返回值:
* 如果是 master, 返回
```
{
    "code": "L200",
    "data": true
}
```
* 如果不是 master(包括没有开启 cluster 功能), 返回
```
{
    "code": "L200",
    "data": false
}
```

### Master API -- 注册slave

```
POST /logkit/cluster/register
{
  "url":"slave_url",
  "tag":"first"
}
```
返回值:
* 如果没有错误, 返回
```
{
    "code": "L200"
}
```
* 如果有错误:
```
{
    "code": <error code>,
    "message": <error message>
}
```

###  Master API -- 获取slave列表

```
GET /logkit/cluster/slaves?tag=tagvalue&url=urlvalue
```

返回值:
* 如果没有错误, 返回
```
{
    "code": "L200",
    "data": [{"url":"http://10.10.0.1:1222","tag":"tag1","status":"ok","last_touch":<rfc3339 string>}]
}
```
* 如果有错误:
```
{
    "code": <error code>,
    "error": <error message>
}
```

* 参数`tag`和`url`非空时将作为被操作`slave`的过滤条件，即上述操作只对满足对应条件的`slave`有效。
* status 状态有三种， ok，表示正常，30s内有联系；bad，表示1分钟内有联系；lost，表示超过1分钟无心跳
* 可以通过tag url参数获取一类tag的列表

### Master API -- 获取slave的 runner name list
```
GET /logkit/cluster/runners?tag=tagvalue&url=urlvalue
```

返回值:
* 如果没有错误, 返回
```
{
    "code": "L200",
    "data": ["runner1", "runner2"]
}
```
* 如果有错误:
```
{
    "code": <error code>,
    "error": <error message>
}
```
* 参数`tag`和`url`非空时将作为被操作`slave`的过滤条件，即上述操作只对满足对应条件的`slave`有效。
* 获取的 runner name list 为各个符合条件的 slave 上的 runner name 的并集

### Master API -- 删除slave

```
DELETE /logkit/cluster/slaves?tag=tagValue&url=urlValue
```
返回值:
* 如果没有错误, 返回
```
{
    "code": "L200"
}
```
* 如果有错误:
```
{
    "code": <error code>,
    "message": <error message>
}
```

注意:
* 参数`tag`和`url`非空时将作为被操作`slave`的过滤条件，即上述操作只对满足对应条件的`slave`有效。
* 该操作进行后，若删除的 slave 再次发心跳注册到 master，则还会出现在 master 的 slave 列表中。

###  Master API -- 获取runner状态列表

```
GET /logkit/cluster/status?tag=tagvalue&url=urlvalue
```
返回值:
* 如果没有错误:
```
Content-Type: application/json

{
  "code": "L200",
  "data":{
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
* 如果有错误:
```
{
    "code": <error code>,
    "error": <error message>
}
```
注意:
* 参数`tag`和`url`非空时将作为被操作`slave`的过滤条件，即上述操作只对满足对应条件的`slave`有效。

###  Master API -- 获取runner 配置文件

```
GET /logkit/cluster/config/<configName>?tag=tagvalue&url=urlvalue
```

默认返回符合`tag`和`url`的第一个 `slave`  的配置文件, 成功时返回:
```
Content-Type: application/json

{
  "code": "L200",
  "data": {
	"name": "xxx",
	... 此处省略，应为 runner config 具体内容
  }
}
```
出现错误时，返回:
```
Content-Type: application/json

{
  "code": "<error code>",
  "message": "<error message>"
}
```

###  Master API -- 获取runner 配置文件

```
GET /logkit/cluster/configs?tag=tagvalue&url=urlvalue
```

返回值:
```
Content-Type: application/json

{
  "code": "L200",
  "data": {
    "url1":{
      "configs":{
        config_file_path: {
          "name": xxx,
          "batch_interval": xx
          ....
          以下略过，此处为 logkit runner 的配置文件
        },
        ....
      },
      "tag":<tag>,
      "error":<error info>
    }
    ...
  }
}

```
* 如果有错误:
```
{
    "code": <error code>,
    "error": <error message>
}
```
注意:
* 参数`tag`和`url`非空时将作为被操作`slave`的过滤条件，即上述操作只对满足对应条件的`slave`有效。

### Slave API -- 注册（修改）标签

```
POST /logkit/cluster/tag
{
  "tag":"first"
}
```
返回值:
* 如果没有错误, 返回
```
{
    "code": "L200"
}
```
* 如果有错误:
```
{
    "code": <error code>,
    "message": <error message>
}
```

修改slave 的标签，slave向master发送心跳时通过register接口顺便做修改

### Master API -- 为 Slave 添加 runner

```
POST /logkit/cluster/configs/<runnerName>?tag=tagValue&url=urlValue
{
    "name": "runner-xxx",
    ....
    以下略过，request body 为 logkit runner 的配置文件
}
```
返回值:
* 如果没有错误, 返回
```
{
    "code": "L200"
}
```
* 如果有错误:
```
{
    "code": <error code>,
    "message": <error message>
}
```
注意:
* 参数`tag`和`url`非空时将作为被操作`slave`的过滤条件，即上述操作只对满足对应条件的`slave`有效。

### Master API -- 为 Slave 更新 runner

```
PUT /logkit/cluster/configs/<runnerName>?tag=tagValue&url=urlValue
{
    "name": "runner-xxx",
    ....
    以下忽略，request body 为 logkit runner 的配置文件
}
```
返回值:
* 如果没有错误, 返回
```
{
    "code": "L200"
}
```
* 如果有错误:
```
{
    "code": <error code>,
    "message": <error message>
}
```
注意:
* 参数`tag`和`url`非空时将作为被操作`slave`的过滤条件，即上述操作只对满足对应条件的`slave`有效。

### Master API -- 为 Slave 删除 runner

```
DELETE /logkit/cluster/configs/<runnerName>?tag=tagValue&url=urlValue
```
返回值:
* 如果没有错误, 返回
```
{
    "code": "L200"
}
```
* 如果有错误:
```
{
    "code": <error code>,
    "message": <error message>
}
```
注意:
* 参数`tag`和`url`非空时将作为被操作`slave`的过滤条件，即上述操作只对满足对应条件的`slave`有效。

### Master API -- 为 Slave 停止 runner

```
POST /logkit/cluster/configs/<runnerName>/stop?tag=tagValue&url=urlValue
```
返回值:
* 如果没有错误, 返回
```
{
    "code": "L200"
}
```
* 如果有错误:
```
{
    "code": <error code>,
    "message": <error message>
}
```
注意:
* 参数`tag`和`url`非空时将作为被操作`slave`的过滤条件，即上述操作只对满足对应条件的`slave`有效。

### Master API -- 为 Slave 启动 runner

```
POST /logkit/cluster/configs/<runnerName>/start?tag=tagValue&url=urlValue
```
返回值:
* 如果没有错误, 返回
```
{
    "code": "L200"
}
```
* 如果有错误:
```
{
    "code": <error code>,
    "message": <error message>
}
```
注意:
* 参数`tag`和`url`非空时将作为被操作`slave`的过滤条件，即上述操作只对满足对应条件的`slave`有效。

### Master API -- 为 Slave 重置 runner

```
POST /logkit/cluster/configs/<runnerName>/reset?tag=tagValue&url=urlValue
```
返回值:
* 如果没有错误, 返回
```
{
    "code": "L200"
}
```
* 如果有错误:
```
{
    "code": <error code>,
    "message": <error message>
}
```
注意:
* 参数`tag`和`url`非空时将作为被操作`slave`的过滤条件，即上述操作只对满足对应条件的`slave`有效。

### Master API -- 为 Slave 设置 tag

```
POST logkit/cluster/slaves/tag?tag=tagValue&url=urlValue
{
  "tag":"first"
}
```
返回值:
* 如果没有错误, 返回
```
{
    "code": "L200"
}
```
* 如果有错误:
```
{
    "code": <error code>,
    "message": <error message>
}
```
注意:
* 参数`tag`和`url`非空时将作为被操作`slave`的过滤条件，即上述操作只对满足对应条件的`slave`有效。
