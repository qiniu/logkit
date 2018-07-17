# logkit Rest API


## Cluster相关API 参阅 [cluser_design](https://github.com/qiniu/logkit/blob/develop/mgr/cluster_design.md)

## Version

### 获取logkit版本号

请求
```
GET /logkit/version
```

返回一个字符串

```
{
    "code": "L200",
    "data": {
        "version":"<版本号>"
    }
}
```

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```

## 错误码含义
请求
```
GET /logkit/errorcode
```
返回
```
Content-Type: application/json

{
    "code": "L200",
    "data": {
        <error code1>: <含义1>,
        <error code2>: <含义2>
        ...
    }
}
```

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```

## Runner

### 获取runner name list
```
GET /logkit/runners
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

### 获取runner运行状态

请求

```
GET /logkit/status
```

返回

```
Content-Type: application/json

{
  "code": "L200",
  "data": {
    <runner_name1>: {
      "name":"runner1",
      "runningStatus": "running",
      "logpath":"/your/log/path1",
      "readDataSize": <读取数据的bytes大小>.
      "readDataCount":<读取数据条数>,
      "elaspedtime":<总用时>,
      "readspeed_kb":<float>,
      "readspeed":<float>,
      "readspeedtrend_kb":<string>,
      "readspeedtrend":<string>,
      "lag":{  
        "size":<lag size>,
        "files":<lag file number>,
        "ftlags":<fault torrent lags>
      },
      "readerStats":{
        "last_error":"error message"
      },
      "parserStats":{  
        "errors":<error number>,
        "success":<success number>,
        "speed":<float>,
        "trend":<string>,
        "last_error":"error message"
      },
      "transformStats":{
        "<transformtype>":{
          "errors":<error number>,
          "success":<success number>,
          "speed":<float>,
          "trend":<string>,
          "last_error":"error message"
        }
      }
      "senderStats":{
        "senderName":{
          "errors":<error number>,
          "success":<success number>,
          "speed":<float>,
          "trend":<string>,
          "last_error":"error message"
        }
      },
      "error":"error msg"
    },
    <runner_name2>: {
      "name":"runner2",
      "logpath":"/your/log/path2",
      "readDataSize": <读取数据的bytes大小>.
      "readDataCount":<读取数据条数>,
      "elaspedtime"<总用时>,
      "readspeed_kb":<float>,
      "readspeed":<float>,
      "readspeedtrend_kb":<string>,
      "readspeedtrend":<string>,
      "lag":{
        "size":<lag size>,
        "files":<lag file number>,
        "ftlags":<fault torrent lags>
      },
      "readerStats":{
        "last_error":"error message"
      },
      "parserStats":{
        "errors":<error number>,
        "success":<success number>,
        "speed":<float>,
        "trend":<string>,
        "last_error":"error message"
      },
      "transformStats":{
        "<transformtype>":{
          "errors":<error number>,
          "success":<success number>,
          "speed":<float>,
          "trend":<string>,
          "last_error":"error message"
        }
      }
      "senderStats":{
        "senderName":{
        "errors":<error number>,
        "success":<success number>,
        "speed":<float>,
        "trend":<string>,
        "last_error":"error message"
      }
    },
    "error":"error msg"
  }
}
```
* "readspeed_kb": 每秒的读取流量大小 KB/s
* "readspeed": 每秒读取记录个数 条/s
* "readspeedtrend_kb": 流量读取速度趋势  "up" 上升,"down" 下降,"stable" 不变
* "readspeedtrend": 记录个数速度读取趋势 "up" 上升,"down" 下降,"stable" 不变
* "speed": 速度 条/s
* "trend": 速度趋势 "up" 上升,"down" 下降,"stable" 不变
* "elaspedtime": 运行时长
* "runningStatus": 当前 runner 的运行状态, "running"表示正在运行, "stopped"表示已停止

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```

### 获取所有runner的出错历史信息

请求

```
GET /logkit/errors
```

返回

```
Content-Type: application/json

{
  "code": "L200",
  "data": {
    <runner_name1>: {
      "read_errors":{
        {
            "error":"<read error info>"",
            "unix_nano_time":<unix nano time>
        },
        ...
      },
      "parse_errors": {
        {
            "error":"<parse error info>"",
            "unix_nano_time":<unix nano time>
        },
        ...
      },
      "transform_errors": {
        {
            "error":"<transform error info>"",
            "unix_nano_time":<unix nano time>
        },
        ...
      },
      "send_errors": {
        {
            "error":"<send error info>"",
            "unix_nano_time":<unix nano time>
        },
        ...
      }
    },
    <runner_name2>: {
      "read_errors":{
        {
            "error":"<read error info>"",
            "unix_nano_time":<unix nano time>
        },
        ...
      },
      "parse_errors": {
        {
            "error":"<parse error info>"",
            "unix_nano_time":<unix nano time>
        },
        ...
      },
      "transform_errors": {
        {
            "error":"<transform error info>"",
            "unix_nano_time":<unix nano time>
        },
        ...
      },
      "send_errors": {
        {
            "error":"<send error info>"",
            "unix_nano_time":<unix nano time>
        },
        ...
      }
    },
    ...
  }
}
```
* "read_errors": read 历史错误数据
* "parse_errors": parse 历史错误数据
* "transform_errors": transform 历史错误数据
* "send_errors": send 历史错误数据
* "error": 具体错误信息
* "unix_nano_time": 错误发生时间
* 默认历史错误信息最多为50条，可以通过"errors_list_cap"设置历史错误信息最大条数，"errors_list_cap"和"batch_interval"在同一个层级

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```

### 获取指定runner的出错历史信息

请求

```
GET /logkit/errors/<name>
```

返回

```
Content-Type: application/json

{
  "code": "L200",
  "data": {
    "read_errors":{
      {
        "error":"<read error info>"",
        "unix_nano_time":<unix nano time>
      },
      ...
    },
    "parse_errors": {
      {
        "error":"<parse error info>"",
        "unix_nano_time":<unix nano time>
      },
      ...
    },
    "transform_errors": {
      {
        "error":"<transform error info>"",
        "unix_nano_time":<unix nano time>
      },
      ...
    },
    "send_errors": {
      {
        "error":"<send error info>"",
        "unix_nano_time":<unix nano time>
      },
      ...
    }
  }
}
```
* "read_errors": read 历史错误数据
* "parse_errors": parse 历史错误数据
* "transform_errors": transform 历史错误数据
* "send_errors": send 历史错误数据
* "error": 具体错误信息
* "unix_nano_time": 错误发生时间
* 默认历史错误信息最多为50条，可以通过"errors_list_cap"设置历史错误信息最大条数，"errors_list_cap"和"batch_interval"在同一个层级

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```

### 获取指定runner运行状态

请求

```
GET /logkit/<runnerName>/status 
```

返回

```
Content-Type: application/json
{
  "code": "L200",
  "data": {
    "name":"runner1",
    "runningStatus": "running",
    "logpath":"/your/log/path1",
    "readDataSize": <读取数据的bytes大小>.
    "readDataCount":<读取数据条数>,
    "elaspedtime"<总用时>,
    "readspeed_kb":<float>,
    "readspeed":<float>,
    "readspeedtrend_kb":<string>,
    "readspeedtrend":<string>,
    "lag":{
      "size":<lag size>,
      "files":<lag file number>,
      "ftlags":<fault torrent lags>
    },
    "readerStats":{
      "last_error":"error message"
    },
    "parserStats":{
      "errors":<error number>,
      "success":<success number>,
      "speed":<float>,
      "trend":<string>,
      "last_error":"error message"
    },
    "transformStats":{
      "<transformtype>":{
        "errors":<error number>,
        "success":<success number>,
        "speed":<float>,
        "trend":<string>,
        "last_error":"error message"
      }
    },
    "senderStats":{
      "senderName":{
        "errors":<error number>,
        "success":<success number>,
        "speed":<float>,
        "trend":<string>,
        "last_error":"error message"
      }
    },
    "error":"error msg"
  }
}
```
* "readspeed_kb": 每秒的读取流量大小 KB/s
* "readspeed": 每秒读取记录个数 条/s
* "readspeedtrend_kb": 流量读取速度趋势  "up" 上升,"down" 下降,"stable" 不变
* "readspeedtrend": 记录个数速度读取趋势 "up" 上升,"down" 下降,"stable" 不变
* "speed": 速度 条/s
* "trend": 速度趋势 "up" 上升,"down" 下降,"stable" 不变
* "elaspedtime": 运行时长
* "runningStatus": 当前 runner 的运行状态, "running"表示正在运行, "stopped"表示已停止

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```

### 添加 Runner

请求

```
POST /logkit/configs/<runnerName>
Content-Type: application/json
{
    "name":"logkit_runner",
    "batch_len": 1000,
    "batch_size": 2097152,
    "batch_interval": 300, 
    "reader":{
        "log_path":"/home/user/app/log/dir/",
        "meta_path":"./metapath",
        "donefile_retention":"7",
        "read_from":"newest",
        "mode":"dir",
        "valid_file_pattern":"qiniulog-*.log" // 可不选，默认为 "*"
    },
     "cleaner":{
        "delete_enable":"true",
        "delete_interval":"10",
        "reserve_file_number":"10",
        "reserve_file_size":"10240"
    },
    "parser":{
        "name":"json_parser",
        "type":"json"
    },
    "senders":[{
        "name":"test_sender",
        "sender_type":"pandora",
        "fault_tolerant":"false",
        "pandora_ak":"your_ak",
        "pandora_sk":"your_sk",
        "pandora_host":"https://pipeline.qiniu.com",
        "pandora_repo_name":"repo_test",
        "pandora_region":"nb",
        "pandora_schema_free":"true"
    }]
}
```


返回

如果请求成功, 返回HTTP状态码200:

```
{
    "code": "L200"
}
```

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```

### 修改 Runner

请求

```
PUT /logkit/configs/<runnerName>
Content-Type: application/json
{
    "name":"logkit_runner",
    "batch_len": 1000,
    "batch_size": 2097152,
    "batch_interval": 300, 
    "reader":{
        "log_path":"/home/user/app/log/dir/",
        "meta_path":"./metapath",
        "donefile_retention":"7",
        "read_from":"newest",
        "mode":"dir",
        "valid_file_pattern":"qiniulog-*.log" // 可不选，默认为 "*"
    },
     "cleaner":{
        "delete_enable":"true",
        "delete_interval":"10",
        "reserve_file_number":"10",
        "reserve_file_size":"10240"
    },
    "parser":{
        "name":"json_parser",
        "type":"json"
    },
    "senders":[{
        "name":"test_sender",
        "sender_type":"pandora",
        "fault_tolerant":"false",
        "pandora_ak":"your_ak",
        "pandora_sk":"your_sk",
        "pandora_host":"https://pipeline.qiniu.com",
        "pandora_repo_name":"repo_test",
        "pandora_region":"nb",
        "pandora_schema_free":"true"
    }]
}
```


返回

如果请求成功, 返回HTTP状态码200:

```
{
    "code": "L200"
}
```

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```

**说明**

1. 修改runner时，请求体需要包含全量的配置
2. 修改runner时，会先把原来的runner移除，然后再添加新的runner。


### 删除 runner

请求

```
DELETE /logkit/configs/<runnerName>
```

返回

如果请求成功, 返回HTTP状态码200:

```
{
    "code": "L200"
}
```

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```

### 重置 runner

请求

```
POST /logkit/configs/<runnerName>/reset
```

返回

如果请求成功, 返回HTTP状态码200:

```
{
    "code": "L200"
}
```

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```
**注意**

**重置runner的作用：**

1. 删除runner
2. 删除runner的meta文件夹
3. 重新启动runner

### 启动 runner

请求

```
POST /logkit/configs/<runnerName>/start
```

返回

如果请求成功, 返回HTTP状态码200:

```
{
    "code": "L200"
}
```

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```

### 停止 runner

请求

```
POST /logkit/configs/<runnerName>/stop
```

返回

如果请求成功, 返回HTTP状态码200:

```
{
    "code": "L200"
}
```

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```
**注意**
停止runner后，前端界面所有的动态归零，但是不会影响到runner的工作进度，runner重新启动后所有的状态都恢复到停止之前。

## Reader

### 获得Reader用途说明

请求

```
GET /logkit/reader/usages
```

返回

```
{
    "code": "L200",
    "data": {
        "readerType1":"reader用途说明1",
        "readerType2":"reader用途说明2"
        ...
    }
}
```

### 获取Reader选项

请求

```
GET /logkit/reader/options
```

返回

```
{
    "code": "L200",
    "data": {
        "<readerType>": [
            {
                "KeyName":      "ReaderKey1",
                "ChooseOnly":   <true/false>,
                "Default":      "default value",
                "DefaultNoUse": <true/false>,
                "Description":  "字段描述",
                "CheckRegex":"<校验字段的正则表达式，为空不校验>"
            },
            {
                "KeyName":      "ReaderKey2",
                "ChooseOnly":   <true/false>,
                "Default":      "default value",
                "DefaultNoUse": <true/false>,
                "Description":  "字段描述",
                "CheckRegex":"<校验字段的正则表达式，为空不校验>"
            }
            ...
        ]
        ...
    }
}
```

* 目前支持的reader type 包括：  "dir" "file" "tailx" "mysql" "mssql" "elastic" "mongo" "kafka" "redis" "socket"


### 校验Reader选项

请求

```
POST /logkit/reader/check
Content-Type: application/json
{
    "mode": "<readerMode>",
    "key2": "value2",
    "key3": "value3",
    "key4": "value4"
}
```

返回

如果校验成功,返回HTTP状态码200:

```
{
    "code": "L200"
}
```

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```


## Parser

### 校验Parser选项

请求

```
POST /logkit/parser/check
Content-Type: application/json
{
    "type": "<parserType>",
    "key2": "value2",
    "key3": "value3",
    "key4": "value4"
}
```

返回

如果校验成功,返回HTTP状态码200:

```
{
    "code": "L200"
}
```

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```

### 尝试解析样例日志

请求

```
POST /logkit/parser/parse
Content-Type: application/json
{
    "sampleLog":"my sample log",
    "type": "<parserType>",
    "key2": "value2",
    "key3": "value3",
    "key4": "value4"
}
```

返回

如果请求成功,返回HTTP状态码200:

```
{
    "code": "L200",
    "data": {
        "SamplePoints":[
            {
                "field1": value1,
                "field2": value2
                ...
            },
            ...
        ]
    }
}
```

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```

### 获得Parser用途说明

请求

```
GET /logkit/parser/usages
Content-Type: application/json
```

返回

```
{
    "code": "L200",
    "data": {
        "parserType1":"parser用途说明1",
        "parserType2":"parser用途说明2"
        ...
    }
}
```

### 获取Parser选项

请求

```
GET /logkit/parser/options
Content-Type: application/json
```

返回

```
{
    "code": "L200",
    "data": {
        "<parserType>": [
            {
                "KeyName":      "ParserKey1",
                "ChooseOnly":   <true/false>,
                "Default":      "<defaultKeyValue>",
                "DefaultNoUse": <true/false>,
                "Description":  "字段描述",
                "CheckRegex":"<校验字段的正则表达式，为空不校验>"
            },
            {
                "KeyName":      "ParserKey2",
                "ChooseOnly":   <true/false>,
                "Default":      "<defaultKeyValue>",
                "DefaultNoUse": <true/false>,
                "Description":  "字段描述",
                "CheckRegex":"<校验字段的正则表达式，为空不校验>"
            }
            ...
        ]
    }
}
```

* 目前支持的parser type 包括： "csv" "qiniulog" "kafkarest" "raw" "empty" "grok" "json" "nginx"


### 获取Parser样例日志

请求

```
GET /logkit/parser/samplelogs
Content-Type: application/json
```

返回

```
{
    "code": "L200",
    "data": {
        "parserType1":"parserType1 样例日志",
        "parserType2":"parserType2 样例日志"
        ...
    }
}
```

## Sender

### 获得 Sender 用途说明

请求

```
GET /logkit/sender/usages
Content-Type: application/json
```

返回

```
{
    "code": "L200",
    "data": {
        "senderType1":"sender用途说明1",
        "senderType2":"sender用途说明2"
        ...
    }
}
```

### 获取 Sender 选项

请求

```
GET /logkit/sender/options
Content-Type: application/json
```

返回

```
{
    "code": "L200",
    "data": {
        "<senderType>": [{
            "KeyName":      "SenderKey",
            "ChooseOnly":   <true/false>,
            "Default":      "<default key value>",
            "DefaultNoUse": <true/false>,
            "Description":  "字段描述",
            "CheckRegex":"<校验字段的正则表达式，为空不校验>"
        },
        {
            "KeyName":      "SenderKey",
            "ChooseOnly":   <true/false>,
            "Default":      "<default key value>",
            "DefaultNoUse": <true/false>,
            "Description":  "字段描述",
            "CheckRegex":"<校验字段的正则表达式，为空不校验>"
        }]
    }
}
```

* 目前支持的sender type包括：
  - "file"          // 本地文件
  - "pandora"       // Pandora
  - "mongodb_acc"   // mongodb 并且按字段聚合
  - "influxdb"      // influxdb
  - "discard"       // discard sender
  - "elasticsearch" // elastic
  

### 校验Sender选项

请求

```
POST /logkit/sender/check
Content-Type: application/json
[{
    "type": "<senderType>",
    "key2": "value2",
    "key3": "value3",
    "key4": "value4"
}]
```

**注意: sender的校验body是个数组**

返回

如果校验成功,返回HTTP状态码200:

```
{
    "code": "L200"
}
```

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```

## Transformer

### 获得 Transformer 用途说明


```
GET /logkit/transformer/usages
Content-Type: application/json
```

返回

```
{
    "code": "L200",
    "data": [
        "transformerType1":{
            "key":"transformerType1",
            "value":"transformer用途说明1"
        }
        "transformerType2":{
           "key": "transformerType2",
           "value":"transformer用途说明2"
        }
    ]
}
```

### 获取 Transformer 选项

请求

```
GET /logkit/transformer/options
Content-Type: application/json
```

返回

```
{
    "code": "L200",
    "data": {
        "IP":[{
             "KeyName":      "IPKey",
             "ChooseOnly":   <true/false>,
             "Default":      "<default key value>",
             "DefaultNoUse": <true/false>,
             "Description":  "字段描述",
             "CheckRegex":"<校验字段的正则表达式，为空不校验>",
             "Type":"<string/long>"
        },
        {
             "KeyName":      "SenderKey",
             "ChooseOnly":   <true/false>,
             "Default":      "<default key value>",
             "DefaultNoUse": <true/false>,
             "Description":  "字段描述",
             "CheckRegex":"<校验字段的正则表达式，为空不校验>",
             "Type":"<string/long>"
        }],
        "replace":[{
           "KeyName":      "IPKey",
           "ChooseOnly":   <true/false>,
           "Default":      "<default key value>",
           "DefaultNoUse": <true/false>,
           "Description":  "字段描述",
           "CheckRegex":"<校验字段的正则表达式，为空不校验>",
           "Type":"<string/long>"
        },
        {
           "KeyName":      "SenderKey",
           "ChooseOnly":   <true/false>,
           "Default":      "<default key value>",
           "DefaultNoUse": <true/false>,
           "Description":  "字段描述",
           "CheckRegex":"<校验字段的正则表达式，为空不校验>",
           "Type":"<string/long>"
        }]
    }
}
```

### 获取 Transformer 样例

请求

```
GET /logkit/transformer/sampleconfigs
Content-Type: application/json
```

返回

一个 map[string]string的接口，key是类型，value是样例配置字符串

```
{
    "code": "L200",
    "data": {
        "IP":{
            "type":"IP",
            "stage":"after_parser",
            "key":"MyIpFieldKey",
            "data_path":"your/path/to/ip.dat"
        },
        "replace":{
            "type":"replace",
            "stage":"before_parser",
            "key":"MyReplaceFieldKey",
            "old":"myOldString",
            "new":"myNewString"
        },
        "date":{
            "type":"date",
            "key":"DateFieldKey",
            "offset":0,
            "time_layout_before":"",
            "time_layout_after":"2006-01-02T15:04:05Z07:00"
        }
    }
}
```

### 尝试转化（解析后的）样例日志

请求

```
POST /logkit/transformer/transform
Content-Type: application/json
{
    "sampleLog":"my sample log(in json string format)",
    "type": "<transformerType>",
    "key2": "value2",
    "key3": "value3",
    ...(other k-v params for a exact transformer)
}
```

**注意: sampleLog应该是JSON字符串（是经过parser解析后的样例日志）。可以是单条样例日志，可以是JSON Array形式的多条日志**

返回

如果请求成功,返回HTTP状态码200:

```
{
    "code": "L200",
    "data": [
        {
            "field1": value1,
            "field2": value2
            ...
        },
        ...
    ]
}
```

**注意: 无论是单条还是多条样例日志，返回值都是数组，数组的每个元素，对应于一条样例日志的转化结果**

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "code":   "<error code>",
    "message": "<error message>"
}
```

## 返回码列表
#### 一切正常

* `L200`: 操作成功

#### logkti 自身 Runner 操作相关

* `L1001`: 获取 Config 出现错误
* `L1002`: 添加 Runner 出现错误
* `L1003`: 删除 Runner 出现错误
* `L1004`: 开启 Runner 出现错误
* `L1005`: 关闭 Runner 出现错误
* `L1006`: 重置 Runner 出现错误
* `L1007`: 更新 Runner 出现错误

#### logkit 自身 Parser 相关

* `L1101`: 解析字符串出现错误

#### logkit 自身 Transformer 相关

* `L1201`: 转化字段出现错误

#### logkit cluster Master 相关

* `L2001`: 获取 Slaves 列表出现错误
* `L2002`: 获取 Slaves 状态出现错误
* `L2003`: 获取 Slaves Configs 出现错误
* `L2004`: 接受 Slaves 注册时出现错误
* `L2014`: 获取 Slaves Config 出现错误

#### logkit cluster slave 自身相关

* `L2005`: 更改 Tag 出现错误

#### logkit cluster Master 管理 slaves 相关

* `L2006`: Slaves 添加 Runner 出现错误
* `L2007`: Slaves 删除 Runner 出现错误
* `L2008`: Slaves 启动 Runner 出现错误
* `L2009`: Slaves 关闭 Runner 出现错误
* `L2010`: Slaves 重置 Runner 出现错误
* `L2011`: Slaves 更新 Runner 出现错误
* `L2012`: Slaves 从列表中移除时出现错误
* `L2013`: Slaves 更改 Tag 出现错误
