# logkit-community [![Build Status](https://api.travis-ci.org/qiniu/logkit.svg)](http://travis-ci.org/qiniu/logkit) [![Go Report Card](https://goreportcard.com/badge/github.com/qiniu/logkit)](https://goreportcard.com/report/github.com/qiniu/logkit) [![codecov](https://codecov.io/gh/qiniu/logkit/branch/master/graph/badge.svg)](https://codecov.io/gh/qiniu/logkit/branch/master) [![GoDoc](https://godoc.org/github.com/qiniu/logkit?status.svg)](https://godoc.org/github.com/qiniu/logkit)

![logkit LOGO](https://raw.githubusercontent.com/qiniu/logkit/master/resources/logo.png)

[中文版](https://github.com/qiniu/logkit/blob/master/READMECN.md)

## Introduce

Very powerful server agent for collecting & sending logs & metrics with an easy-to-use web console.

### logkit-community Detail doc can be referred to[WIKI](https://github.com/qiniu/logkit/wiki)

## Support sources

* [File](https://github.com/qiniu/logkit/wiki/File-Reader): read data in file, including csv file，kafka-rest log，nginx log.
* [Elasticsearch](https://github.com/qiniu/logkit/wiki/ElasticSearch-Reader): read data in ElasticSearch.
* [MongoDB](https://github.com/qiniu/logkit/wiki/MongoDB-Reader): read data in MongoDB.
* [MySQL](https://github.com/qiniu/logkit/wiki/MySQL-Reader): read data in MySQL.
* [MicroSoft SQL Server](https://github.com/qiniu/logkit/wiki/MicroSoft-SQL-Server-Reader): read data in Microsoft SQL Server.
* [Postgre SQL](https://github.com/qiniu/logkit/wiki/PostgreSQL-Reader): read data in PostgreSQL.
* [Kafka](https://github.com/qiniu/logkit/wiki/Kafka-Reader): read data in Kafka.
* [Redis](https://github.com/qiniu/logkit/wiki/Redis-Reader): read data in Redis.
* [Socket](https://github.com/qiniu/logkit/wiki/Socket-Reader): read data via tcp\udp\unixsocket protocol.
* [Http](https://github.com/qiniu/logkit/wiki/Http-Reader): reveive data in post request as http server.
* [Script](https://github.com/qiniu/logkit/wiki/Script-Reader): support script and read data from the result.
* [Snmp](https://github.com/qiniu/logkit/wiki/Snmp-Reader): auto read data from Snmp service.

## Working method

logkit-community support multiple sources and can send kinds of data to Pandora, every data source relevant to a logic runner,a runner's workaround as follows:

![logkit workaround](https://raw.githubusercontent.com/qiniu/logkit/master/resources/logkit.png)

## Contributing

Weclome to contribute to logkit:

* fix or[report bug](https://github.com/qiniu/logkit/issues/new)
* [pull issue](https://github.com/qiniu/logkit/issues/new)improve[wiki doc](https://github.com/qiniu/logkit/wiki)
* [review code](https://github.com/qiniu/logkit/pulls)or[提出功能需求](https://github.com/qiniu/logkit/issues/new)
* contribute code (contribute kinds of modules including[reader](https://github.com/qiniu/logkit/wiki/Readers)、[parser](https://github.com/qiniu/logkit/wiki/Parsers)、[sender](https://github.com/qiniu/logkit/wiki/Senders)以及[transformer](https://github.com/qiniu/logkit/wiki/Transformers)）

## Download

**lastest stable**：Go to[Download page](https://github.com/qiniu/logkit/wiki/Download)

**History**：Go to[Releases](https://github.com/qiniu/logkit/releases)

**Trial**：construct lastest logkit trial version every 5:00am (only for Linux 64 and Docker), you can download it (note: not include update of frontend).

* [Linux 64 download](https://pandora-dl.qiniu.com/nightly/logkit_nightly.tar.gz)

* Docker image nightly:  `docker pull wonderflow/logkit:nightly`

## Install and Usage

### 1. Download&Decompress logkit-community tool

* Linux

``` sh
export LOGKIT_VERSION=<version number>
wget https://pandora-dl.qiniu.com/logkit_${LOGKIT_VERSION}.tar.gz && tar xvf logkit_${LOGKIT_VERSION}.tar.gz && rm logkit_${LOGKIT_VERSION}.tar.gz && cd _package_linux64/
```

* MacOS

``` sh
export LOGKIT_VERSION=<version number>
wget https://pandora-dl.qiniu.com/logkit_mac_${LOGKIT_VERSION}.tar.gz && tar xvf logkit_mac_${LOGKIT_VERSION}.tar.gz && rm logkit_mac_${LOGKIT_VERSION}.tar.gz && cd _package_mac/
```

* Windows

please download `https://pandora-dl.qiniu.com/logkit_windows_<LOGKIT_VERSION>.zip` 并解压缩，go to directory

### 2. change logkit-community configuration

logkit.conf is logkit-community tool's configuration，mainly for specifing running resource and paths of runners.

Open `logkit.conf`, for example：

``` json
{
    "max_procs": 8,
    "debug_level": 1,
    "clean_self_log":true,
    "bind_host":"localhost:3000",
    "static_root_path":"./public",
    "confs_path": ["confs*"]
}
```

For simply use, you can only focus on three options:

1. `bind_host` port of logkit we。
1. `static_root_path` statistic resource path of logkit page, **recommand to use absolute path** note：old version moved to "public-old" directory。
1. `confs_path` including add conf in web, logkit also support monitor directory to add runners. (if you only need to add logkit runner in web, you can ignore this option)


### 3. startup logkit-community tool

``` sh
./logkit -f logkit.conf
```

### 4. Open logkit-community config page in web

the web url is the value of `bind_host` configured in step 2

## Contribute frontend code

refer to README file：[logkitweb/README.md](https://github.com/qiniu/logkit/blob/master/logkitweb/README.md)

## Install and startup from source code

``` sh
go build -o logkit logkit.go
./logkit -f logkit.conf
```

## startup logkit using docker

``` sh
docker pull wonderflow/logkit:<version>
docker run -d -p 3000:3000 -v /local/logkit/dataconf:/app/confs -v /local/log/path:/logs/path logkit:<version>
```

## Deploying logkit in Kubernetes

get configs deploying in Kubernetes

``` sh
curl -L -O https://raw.githubusercontent.com/qiniu/logkit/master/deploy/logkit_on_k8s.yaml
```

enjoy it！
