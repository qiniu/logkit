
# logkit [![Build Status](https://api.travis-ci.org/qiniu/logkit.svg)](http://travis-ci.org/qiniu/logkit)

## 简介

logkit是[七牛Pandora](https://pandora-docs.qiniu.com)开发的一个通用的日志收集工具，可以将不同数据源的数据方便的发送到[Pandora](https://pandora-docs.qiniu.com)进行数据分析，除了基本的数据发送功能，logkit还有容错、并发、监控、删除等功能。

### logkit 详细的文档可以参见[WIKI](https://github.com/qiniu/logkit/wiki)页面


## 支持的数据源

1. 文件(包括csv格式的文件，kafka-rest日志文件，nginx日志文件等,并支持以[grok](https://www.elastic.co/blog/do-you-grok-grok)的方式解析日志)
1. MySQL
1. Microsoft SQL Server(MS SQL)
1. Elasticsearch
1. MongoDB
1. Kafka
1. Redis

## 工作方式

logkit本身支持多种数据源，并且可以同时发送多个数据源的数据到Pandora，每个数据源对应一个逻辑上的runner，一个runner负责一个数据源的数据推送，工作原理如下图所示

![logkit 工作原理图](https://qiniu.github.io/pandora-docs/_media/logkit.png)

## 下载

请移步至[Download页面](https://github.com/qiniu/logkit/wiki/Download)

## 安装与使用


1. 下载&解压logkit工具

**Linux 版本**

```
wget https://pandora-dl.qiniu.com/logkit.tar.gz && tar xvf logkit.tar.gz && cd _package_linux64/
```

**MacOS 版本**

```
brew install wget
wget https://pandora-dl.qiniu.com/logkit_mac.tar.gz && tar xvf logkit_mac.tar.gz && cd _package_mac/
```

**Windows 版本**

请下载 https://pandora-dl.qiniu.com/logkit_windows.zip 并解压缩，进入目录

2. 修改logkit基本配置

```
打开 logkit.conf
```

logkit.conf是logkit工具基础配置文件，主要用于指定logkit运行时需要的资源和各个runner配置文件的具体路径。


典型的配置如下：


```
{
	"max_procs": 8,
	"debug_level": 1,
	"clean_self_log":true,
	"bind_host":"localhost:3000",
	"static_root_path":"./public",
	"confs_path": ["confs*"]
}
```

初步使用，你只需要关注并根据实际需要修改其中三个选项：

1. `bind_host` logkit页面绑定的端口后，启动后可以根据这个页面配置logkit。
1. `static_root_path` logkit页面的静态资源路径，**强烈建议写成绝对路径** 注意：老版本的移动到了 "public-old"文件夹。
1. `confs_path` 除了通过页面配置添加以外，logkit还支持直接监控文件夹添加runner。（如果你只通过页面添加logkit runner，那么无需修改此配置）
列表中的每一项都是一个runner的配置文件夹，如果每一项中文件夹下配置发生增加、减少或者变更，logkit会相应的增加、减少或者变更runner，配置文件夹中的每个配置文件都代表了一个runner。该指定了一个runner的配置文件夹，这个配置文件夹下面每个以.conf结尾的文件就代表了一个运行的runner，也就代表了一个logkit正在运行的推送数据的线程。


3. 启动logkit工具

```
./logkit -f logkit.conf
```

4. 通过浏览器打开logkit配置页面

浏览器访问的地址就是您在第2步中填写的 `bind_host` 选项地址

* 首页查看正在运行的logkit状态，或者添加新的logkit Runner

![查看并添加](https://raw.githubusercontent.com/qiniu/logkit/develop/resources/logkitnewconfig1.png)

* 根据页面配置数据源、配置解析方式、配置发送方式

![配置数据源](https://raw.githubusercontent.com/qiniu/logkit/develop/resources/logkitnewconfig2.png)

* 在配置解析方式的页面您还可以根据配置尝试解析您的样例数据

![尝试解析](https://raw.githubusercontent.com/qiniu/logkit/develop/resources/logkitnewconfig3.png)

* 最后在确认并添加页面点击生成配置文件，再点击添加Runner即可生效

![添加runner](https://raw.githubusercontent.com/qiniu/logkit/develop/resources/logkitnewconfig4.png)


## 前端代码变动

参见前端相关README文件：[logkitweb/README.md](https://github.com/qiniu/logkit/blob/develop/logkitweb/README.md)

## 从源码安装与启动

启动服务的命令中可以指定服务的启动配置

```
go get -u github.com/kardianos/govendor
govendor sync
go get ./...
go build -o logkit logkit.go
./logkit -f logkit.conf
```

## 使用logkit的docker镜像启动

```
docker pull wonderflow/logkit
docker run -d -p 3000:3000 -v /local/logkit/dataconf:/app/confs -v /local/log/path:/logs/path logkit
```

镜像中，logkit读取`/app/confs`下的配置文件，所以可以通过挂载目录的形式，将本地的logkit配置目录`/local/logkit/dataconf`挂载到镜像里面。

需要注意的是，镜像中的logkit收集 `/logs`目录下的日志，需要把本地的日志目录也挂载到镜像里面去才能启动，比如本地的日志目录为`/local/log/path`, 挂载到镜像中的`/logs/path`目录，那么`/local/logkit/dataconf`目录下的配置文件填写的日志路径必须是`/logs/path`。

Enjoy it！
