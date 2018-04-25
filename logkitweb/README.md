#author zhonghuiping

# logkitWeb

[![React](https://img.shields.io/badge/react-^15.6.1-brightgreen.svg?style=flat-square)](https://github.com/facebook/react)
[![Ant Design](https://img.shields.io/badge/ant--design-^2.9.0-yellowgreen.svg?style=flat-square)](https://github.com/ant-design/ant-design)


## 基于react ＋ antd 实现的配置工具助手

   简单,UI友好,使用和维护起来方便。

## 开发构建

### 项目结构

```bash
├── README.md
├── build                                # 项目输出目录
│   ├── asset-manifest.json
│   ├── favicon.ico
│   ├── index.html
│   ├── manifest.json
│   ├── service-worker.js
│   └── static
│       ├── antd.css
│       ├── css
│       │   ├── main.19bfac5c.css
│       │   └── main.19bfac5c.css.map
│       ├── favicon.ico
│       ├── js
│           ├── main.4905a907.js
│           └── main.4905a907.js.map
├── package.json                         # 项目信息
├── public                               # 公共文件夹，编译时copy至build目录
│   ├── favicon.ico
│   ├── index.html
│   ├── manifest.json
│   └── static
│       ├── antd.css
│       ├── favicon.ico
├── src                                  # 源码目录
│   ├── components                       # 配置页 UI 组件
│   │   ├── metricConfig.js
│   │   ├── metricKeys.js
│   │   ├── metricUsages.js
│   │   ├── parserConfig.js
│   │   ├── renderConfig.js
│   │   ├── senderConfig.js
│   │   ├── sourceConfig.js
│   │   └── transformer.js
│   ├── container                       # 列表和配置页容器组件
│   │   ├── createLogContainer.js
│   │   ├── createMetricContainer.js
│   │   └── listContainer.js
│   ├── index.css                       # 全局样式文件
│   ├── index.js                        # 入口文件
│   ├── services                        # 数据接口
│   │   └── logkit.js
│   ├── store                           # 数据仓库
│   │   └── config.js
│   └── utils                           # 工具函数
│       └── request.js
└── yarn.lock                           # yarn 包依赖管理
```

### 快速开始

克隆项目文件:

```bash
git clone https://github.com/qiniu/logkit.git
```

进入目录安装依赖:
cd logkit/logkitweb

```bash
#开始前请确保安装了npm 或者 yarn (npm在安装node的时候就自带了 可以 npm -v查看版本信息)
#安装yarn  npm install -g yarn
npm i 或者 yarn install
```

开发：

```bash
yarn start
# 注意: (package.json里面的  "proxy": "http://localhost:3000" 必须跟 logkit.conf里面的bind_host的端口相同)
打开 http://localhost:3000/
```

构建：

```bash
yarn build

将会打包至当前的build目录
