LogkitRelease
===

## 功能
编译指定路径的 logkit 代码源文件, 压缩打包后, 根据配置发送到不同的目的地

压缩包的目录结构
```
_package/
    logkit
    public/
        ...
    logkit.conf
```

## 使用方法
### 安装
```
pip install -r requirements.txt
```
### 启动
```
python main.py -f config.conf
```
* 注意: 该程序暂未根据 version 自动将 git 切换到相应的 tag, 在运行该程序前请自行将代码切换到相应的 tag

## 依赖
* python 2.X
* qiniu sdk 7.2.0

* 如果担心与系统已有的依赖版本冲突，可以考虑使用 `virtualenv`

## 配置
### 代码相关(code)
1. i386: boolean值, 是否编译 32 位版本
1. amd64: boolean值, 是否编译 64 位版本
1. path: string值, 需要编译的源代码路径(绝对路径)

### 打包相关(pack)
1. i386_name: string值, 32 位版本的压缩包名称
1. amd64_name: string值, 64 位版本的压缩包名称
1. content: string值, 压缩包内的文件(夹)名称, 多个名称使用','隔开

### 压缩包的发送(sender)
1. kodo: boolean值, 是否发送到 kodo
1. dir: boolean值, 是否发送到某个文件夹内

### kodo配置(sender_kodo)
1. ak/sk: string值, 鉴权使用
1. bucket: string值, 存储空间名称

### dir配置(sender_dir)
1. dst_path: string值, 发送到的目的文件夹(绝对路径)


## 配置实例
### mac 版本
```
[code]
i386=false
amd64=true
path=/Users/user/project/gopath/src/github.com/qiniu/logkit

[pack]
i386_name=logkit_mac_%s.tar.gz
amd64_name=logkit_mac_%s.tar.gz
content=logkit,public,logkit.conf,template_confs

[sender]
kodo=true
dir=false

[sender_kodo]
bucket=logkit
ak=xxx
sk=xxx

[sender_dir]
dst_path=/Users/user/test
```

### linux 版本
```
[code]
i386=true
amd64=true
path=/home/user/project/gopath/src/github.com/qiniu/logkit

[pack]
i386_name=logkit_linux32_%s.tar.gz
amd64_name=logkit_%s.tar.gz
content=logkit,public,logkit.conf,template_confs

[sender]
kodo=true
dir=false

[sender_kodo]
bucket=logkit
ak=xxx
sk=xxx

[sender_dir]
dst_path=/Users/user/test
```
### windows 版本
```
[code]
i386=true
amd64=true
path=D:\gopath\src\github.com\qiniu\logkit

[pack]
i386_name=logkit_windows32_%s.zip
amd64_name=logkit_windows_%s.zip
content=logkit.exe,public,logkit.conf,template_confs

[sender]
kodo=true
dir=false

[sender_kodo]
bucket=logkit
ak=xxx
sk=xxx

[sender_dir]
dst_path=D:\test
```
