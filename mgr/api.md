# logkit Rest API


## Runner

### 获取runner运行状态

请求

```
GET /logkit/status
Content-Type: application/json
```

返回

```
{  
   {  
      "name":"runner1",
      "logpath":"/your/log/path1",
      "readDataSize": <读取数据的bytes大小>.
      "readDataCount":<读取数据条数>,
      "elaspedtime"<总用时>,
      "lag":{  
         "size":<lag size>,
         "files":<lag file number>,
         "ftlags":<fault torrent lags>
      },
      "parserStats":{  
         "errors":<error number>,
         "success":<success number>,
         "last_error":"error message"
      },
      "transformStats":{
          "<transformtype>":{
             "errors":<error number>,
             "success":<success number>,
             "last_error":"error message"
           }
      }
      "senderStats":{
         "senderName":{
           "errors":<error number>,
           "success":<success number>,
           "last_error":"error message"
         }
      },
      "error":"error msg"
   },
   {  
      "name":"runner2",
      "logpath":"/your/log/path2",
      "readDataSize": <读取数据的bytes大小>.
        "readDataCount":<读取数据条数>,
        "elaspedtime"<总用时>,
        "lag":{  
           "size":<lag size>,
           "files":<lag file number>,
           "ftlags":<fault torrent lags>
        },
        "parserStats":{  
           "errors":<error number>,
           "success":<success number>,
           "last_error":"error message"
        },
        "transformStats":{
            "<transformtype>":{
               "errors":<error number>,
               "success":<success number>,
               "last_error":"error message"
             }
        }
        "senderStats":{
           "senderName":{
             "errors":<error number>,
             "success":<success number>,
             "last_error":"error message"
           }
        },
        "error":"error msg"
     }
}

```

### 获取指定runner运行状态

请求

```
GET /logkit/<runnerName>/status 
Content-Type: application/json
```

返回

```
{  
   "name":"<runnerName>",
   "logpath":"/your/log/path1",
   "lag":{  
      "size":<lag size>,
      "files":<lag file number>,
      "ftlags":<fault torrent lags>
   },
   "parserStats":{  
      "errors":<error number>,
      "success":<success number>,
      "last_error":"error message"
   },
   "senderStats":{  
      "errors":<error number>,
      "success":<success number>,
      "last_error":"error message"
   },
   "error":"error msg"
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
{}
```

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "error":   "<error message string>"
}
```

### 删除 runner

请求

```
DELETE /logkit/configs/<runnerName>
Content-Type: application/json
```

返回

如果请求成功, 返回HTTP状态码200:

```
{}
```

如果请求失败, 返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "error":   "<error message string>"
}
```

## Reader

### 获得Reader用途说明

请求

```
GET /logkit/reader/usages
Content-Type: application/json
```

返回

```
{
    "readerType1":"reader用途说明1",
    "readerType2":"reader用途说明2"
}
```

### 获取Reader选项

请求

```
GET /logkit/reader/options
Content-Type: application/json
```

返回

```
{
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
    ]
}
```

* 目前支持的reader type 包括：  "dir" "file"  "tailx"  "mysql"    "mssql" "elastic"   "mongo"  "kafka"    "redis"


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
{}
```

如果请求失败,返回包含如下内容的JSON字符串：

```
{
    "error": "<error message string>"
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
{}
```

如果请求失败,返回包含如下内容的JSON字符串：

```
{
    "error": "<error message  string>"
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
{}
```

如果请求失败,返回包含如下内容的JSON字符串（已格式化,便于阅读）:

```
{
    "error":   "<error message string>"
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
    "parserType1":"parser用途说明1",
    "parserType2":"parser用途说明2"
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
    ]
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
    "parserType1":"parserType1 样例日志",
    "parserType2":"parserType2 样例日志"
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
    "senderType1":"sender用途说明1",
    "senderType2":"sender用途说明2"
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
    "<senderType>": [
        {
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
        }
    ]
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
{}
```

如果请求失败,返回包含如下内容的JSON字符串：

```
{
    "error": "<error message string>"
}
```

## Transformer