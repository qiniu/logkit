import React, {Component} from 'react';
import moment from 'moment'
import ClipboardButton from 'react-clipboard.js';
import {
  getRunnerConfigs, getClusterRunnerConfigs, deleteConfigData, getClusterSlaves, getRunnerStatus, getClusterRunnerStatus, getRunnerVersion, resetConfigData, startRunner, stopRunner
} from '../services/logkit';
import _ from "lodash";
import {Table, Icon, Popconfirm, Button, notification, Modal, Row, Col, Tag, Input, Layout, Menu, Breadcrumb } from 'antd';
const SubMenu = Menu.SubMenu;
const { Header, Content, Footer, Sider } = Layout;

class List extends Component {
  constructor(props) {
    super(props);
    this.state = {
      runners: [],
      status: [],
      isShow: false,
      isShowConfig: false,
      isShowResetConfig: false,
      currentItem: '',
      version: '',
      collapsed: false,
      currentMenu: 'tag',
      machines: []
    };
    this.init()
  }

  componentDidMount() {

  }

  componentWillUnmount() {

  }

  componentDidUpdate(prevProps) {

  }

  onCollapse = (collapsed) => {
    //console.log(collapsed);
    this.setState({ collapsed });
  }

  transform = (srcData) => {
      let dataArray = []
      _.forIn(srcData,(value, key) => {
        //console.log(value)
      let runners = [];
      if (value.configs != null) {
        _.forIn(value.configs,(v, k) => {  runners.push(v); })
      }
      dataArray.push({
        machineUrl: key,
        configs: runners,
        error: value.error,
        tag: value.tag
      })

    })
    //console.log(dataArray)
    return dataArray
  }

  transformStatus = (srcData) => {
    let dataArray = []
    _.forIn(srcData,(value, key) => {
      let runnerStatus = [];
      if (value.status !== null) {
        _.forIn(value.status,(v, k) => {  runnerStatus.push(v); })
      }
      dataArray.push({
        machineUrl: key,
        runnerStatus: runnerStatus,
        error: value.error,
        tag: value.tag
      })

    })
    //console.log(dataArray)
    return dataArray
  }


  getStatus = () => {
    let that = this
    getClusterRunnerConfigs().then(data => {
      if (data.success) {
        //console.log(_.omit(data, 'success'))
        let mapData = _.omit(data, 'success')
        let tagMapData = this.transform(mapData)
        that.setState({
          runners: tagMapData
        })
        getClusterRunnerStatus().then(data => {
          if (data.success) {
            that.setState({
              status: this.transformStatus(_.omit(data, 'success'))
            })
          }
        })
      }
    })

    // getRunnerConfigs().then(data => {
    //   if (data.success) {
    //     //console.log(_.omit(data, 'success'))
    //     let mapData = _.omit(data, 'success')
    //     //let tagMapData = this.transform(mapData)
    //     that.setState({
    //       runners: _.omit(data, 'success')
    //     })
    //     getRunnerStatus().then(data => {
    //       if (data.success) {
    //         that.setState({
    //           status: _.values(_.omit(data, 'success'))
    //         })
    //       }
    //     })
    //   }
    // })

    getClusterSlaves().then(data => {
      if (data.success) {
        //console.log(data)
        that.setState({
          machines: _.values(_.omit(data, 'success'))
        })
        //console.log(this.state.machines)
      }
    })
  }

  init = () => {
    let that = this
    getRunnerVersion().then(data => {
      if (data.success) {
        that.setState({
          version: _.values(_.omit(data, 'success'))
        })
      }
    })

    that.getStatus()

    if (window.statusInterval !== undefined && window.statusInterval !== 'undefined') {
      window.clearInterval(window.statusInterval);
    }
    window.statusInterval = setInterval(function () {
      that.getStatus()
    }, 5000)

  }

  optRunner = (record) => {
    if(record.iconType === 'caret-right'){
      startRunner({name:record.name}).then(data => {
        if(!data){
          record.iconType = 'poweroff'
          notification.success({message: '开启成功', duration: 10})
        }
      })
    }else{
      stopRunner({name:record.name}).then(data => {
        if(!data){
          record.iconType = 'caret-right'
          notification.success({message: '关闭成功', duration: 10})
        }
      })
    }
  }

  deleteRunner = (record) => {
    deleteConfigData({name: record.name}).then(data => {
      if(!data){
        notification.success({message: "删除成功", duration: 10,})
        this.init()
      }
    })
  }


  turnToConfigPage() {
    this.props.router.push({pathname: `/index/create?copyConfig=true`})
  }

  isShow = (currentItem) => {
    this.setState({
      currentItem: currentItem,
      isShow: true
    })
  }

  showConfig = (currentItem) => {
    this.setState({
      currentItem: currentItem,
      isShowConfig: true
    })
  }

  showResetConfig = (currentItem) => {
    this.setState({
      currentItem: currentItem,
      isShowResetConfig: true
    })
  }

  handleResetConfigCancel = () => {
    this.setState({
      isShowResetConfig: false
    })
  }

  handleResetConfig = () => {
    resetConfigData({name: this.state.currentItem.name}).then(data => {
      if(!data){
        notification.success({message: "重置成功", duration: 10,})
      }
      this.setState({
        isShowResetConfig: false
      })
      this.init()
    })
  }

  handleErrorCancel = () => {
    this.setState({
      isShow: false
    })
  }

  handleConfigCancel = () => {
    this.setState({
      isShowConfig: false
    })
  }

  renderArrow = (number, trend) => {
    if (trend == "up") {
      return (<div>{number}<Icon style={{color: "#00a854"}} type="arrow-up"/></div>)
    } else if (trend == "down") {
      return (<div>{number}<Icon style={{color: "#f04134"}} type="arrow-down"/></div>)
    } else {
      return (<div>{number}<Icon style={{color: "#ffbf00"}} type="minus"/></div>)
    }
  }

  copyConfig = (record) => {
    window.nodeCopy = record
    notification.success({message: "修改配置文件,", description: '按步骤去修改配置页面的Runner信息', duration: 10,})
    if(record["metric"] === undefined){
      this.props.router.push({pathname: `/index/create-log-runner?copyConfig=true`})
    }else{
      this.props.router.push({pathname: `/index/create-metric-runner?copyConfig=true`})
    }
  }

  renderTagList() {
    let dataSource = []

    this.state.runners.map((item) => {
      //console.log(item)
      dataSource.push({
        name: item.tag,
        machineUrl: item.machineUrl,
        error: item.error
      })
    })
    //console.log(dataSource)

    const columns = [{
      title: '标签名称',
      dataIndex: 'name',
      key: 'name',
    }, {
      title: '机器地址',
      dataIndex: 'machineUrl',
      key: 'machineUrl',
    }, {
      title: '错误信息',
      dataIndex: 'error',
      key: 'error',
    }, {
      title: '添加runner',
      key: 'edit',
      dataIndex: 'edit',
      width: '10%',
      render: (text, record) => {
        return (
            (<a>
              <div className="editable-row-operations">
                {
                  <Icon onClick={this.addLogRunner} style={{fontSize: 16}} type="plus"/>
                }
              </div>
            </a>)
        );
      },

    }];
    return (
            <Table columns={columns} pagination={{size: 'small', pageSize: 20}} dataSource={dataSource}/>
        )
  }

  renderMachineList() {
    let dataSource = []
    //console.log(this.state.machines)
    this.state.machines.map((item) => {
      dataSource.push({
        name: item.tag,
        machineUrl: item.url,
        status: item.status,
        last_touch: item.last_touch
      })
    })
    //console.log(dataSource)

    const columns = [ {
      title: '机器地址',
      dataIndex: 'machineUrl',
      key: 'machineUrl',
    },{
      title: '标签名称',
      dataIndex: 'name',
      key: 'name',
    }, {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
    }, {
      title: '心跳时间',
      dataIndex: 'last_touch',
      key: 'last_touch',
    }, {
      title: '添加runner',
      key: 'edit',
      dataIndex: 'edit',
      width: '10%',
      render: (text, record) => {
        return (
            (<a>
              <div className="editable-row-operations">
                {
                  <Icon  onClick={this.addLogRunner} style={{fontSize: 16}} type="plus"/>
                }
              </div>
            </a>)
        );
      },

    }];
    return (
        <Table columns={columns} pagination={{size: 'small', pageSize: 20}} dataSource={dataSource}/>
    )
  }

  renderRunnerList() {
    const columns = [{
      title: '名称',
      dataIndex: 'name',
      width: '10%'
    },{
      title: '修改时间',
      dataIndex: 'createTime',
      width: '10%'
    }, {
      title: '运行状态',
      dataIndex: 'status',
      width: '5%'
    }, {
      title: '读取总条数',
      dataIndex: 'readDataCount',
      width: '5%'
    }, {
      title: '读取条数(条/秒)',
      dataIndex: 'readerNumber',
      width: '7%',
      render: (text, record) => {
        return (
          this.renderArrow(text, record.readerTrend)
        );
      },
    }, {
      title: '读取速率(KB/s)',
      dataIndex: 'readerkbNumber',
      width: '7%',
      render: (text, record) => {
        return (
          this.renderArrow(text, record.readerkbTrend)
        );
      },
    }, {
      title: '发送速率(条/s)',
      dataIndex: 'sendNumber',
      width: '7%',
      render: (text, record) => {
        return (
          this.renderArrow(text, record.sendTrend)
        );
      },
    }, {
      title: '解析成功/总 (条数)',
      dataIndex: 'parseSuccessOfTotal',
      width: '8%'
    }, {
      title: '发送成功/总 (条数)',
      dataIndex: 'successOfTotal',
      width: '8%'
    }, {
      title: '路径',
      dataIndex: 'path',
      width: '8%'
    }, {
      title: '错误日志',
      dataIndex: 'errorLog',
      width: '8%',
      render: (text, record) => {
        return (
          <a>
            <div className="editable-row-operations">
              {
                <Button type="primary" onClick={() => this.isShow(record)}>查看错误日志</Button>
              }
            </div>
          </a>
        );
      },

    }, {
      title: '详细配置',
      dataIndex: 'config',
      width: '6%',
      render: (text, record) => {
        return (
          <a>
            <div className="editable-row-operations">
              {
                <Button type="primary" onClick={() => this.showConfig(record)}>查看配置</Button>
              }
            </div>
          </a>
        );
      },
    }, {
      title: '编辑',
      key: 'copy',
      dataIndex: 'copy',
      width: '3%',
      render: (text, record) => {
        return (
          <a>
            <div className="editable-row-operations">
              <ClipboardButton data-clipboard-text={text}>
                <Icon style={{fontSize: 16}} onClick={() => this.copyConfig(record.currentItem)} type="edit"/>
              </ClipboardButton>
            </div>
          </a>
        );
      },
    }, {
      title: '操作',
      key: 'opt',
      dataIndex: 'opt',
      width: '3%',
      render: (text, record) => {
        return (
          record.isWebFolder === true ? (<a>
            <div className="editable-row-operations">
              {
                <Popconfirm title={"是否"+record.runnerOpt+"该Runner?"} onConfirm={() => this.optRunner(record)}>
                  <Icon title={record.runnerOpt+"Runner"} style={{fontSize: 16}} type={record.iconType}/>
                </Popconfirm>
              }
            </div>
          </a>): null
        );
      },
    }, {
      title: '重置',
      dataIndex: 'reset',
      width: '3%',
      render: (text, record) => {
        return (
          record.isWebFolder === true ? (<a>
            <div className="editable-row-operations">
              {
                <Icon style={{fontSize: 16}} onClick={() => this.showResetConfig(record)} type="reload"/>
              }
            </div>
          </a>): null
        );
      },
    }, {
      title: '删除',
      key: 'edit',
      dataIndex: 'edit',
      width: '3%',
      render: (text, record) => {
        return (
          record.isWebFolder === true ? (<a>
            <div className="editable-row-operations">
              {
                <Popconfirm title="是否删除该Runner?" onConfirm={() => this.deleteRunner(record)}>
                  <Icon style={{fontSize: 16}} type="delete"/>
                </Popconfirm>
              }
            </div>
          </a>) : null
        );
      },

    }];

    let data = []
    console.log(this.state.runners)
    console.log(this.state.status)
    if (this.state.runners != null) {
      this.state.runners[0].configs.map((item) => {
        let status = '异常'
        let createTime = ''
        let parseSuccessNumber = 0
        let parseFailNumber = 0
        let successNumber = 0
        let readerNumber = 0
        let readDataCount = 0
        let readerkbNumber = 0
        let failNUmber = 0
        let logpath = ''
        let sendNumber = 0
        let parseNumber = 0
        let isWebFolder = false
        let readerError = ''
        let parseError = ''
        let sendError = ''
        let logkitError = ''
        let sendTrend = 'stable'
        let parseTrend = 'stable'
        let readerTrend = 'stable'
        let readerkbTrend = 'stable'
        let iconType = 'poweroff'
        let runnerOpt = '关闭'

        if(item["is_stopped"]){
          status = '关闭'
          runnerOpt = '开启'
          iconType = 'caret-right'
          isWebFolder = item.web_folder
        }

        this.state.status[0].runnerStatus.map((ele) => {
          if (item.name === ele.name) {
            status = '正常'
            createTime = moment(item.createtime).format("YYYY-MM-DD HH:mm:ss")
            parseSuccessNumber = ele.parserStats.success
            parseFailNumber = ele.parserStats.errors
            successNumber = _.values(ele.senderStats)[0] == undefined ? 0 : _.values(ele.senderStats)[0].success
            failNUmber = _.values(ele.senderStats)[0] == undefined ? 0 : _.values(ele.senderStats)[0].errors
            sendNumber = _.floor((_.values(ele.senderStats)[0] == undefined ? 0 : _.values(ele.senderStats)[0].speed), 3)
            parseNumber = _.floor(ele.parserStats.speed, 3)
            readDataCount = ele.readDataCount
            readerNumber = _.floor(ele.readspeed, 3)
            readerkbNumber = _.floor(ele.readspeed_kb, 3)
            sendTrend = _.values(ele.senderStats)[0] == undefined ? '' : _.values(ele.senderStats)[0].trend
            parseTrend = ele.parserStats.trend
            readerTrend = ele.readspeedtrend
            readerkbTrend = ele.readspeedtrend_kb
            logpath = ele.logpath
            readerError = ele.readerStats.last_error
            parseError = ele.parserStats.last_error
            sendError = _.values(ele.senderStats)[0] == undefined ? '' : _.values(ele.senderStats)[0].last_error
            isWebFolder = item.web_folder
            logkitError = ele.error
          }
        })
        data.push({
          key: item.name,
          name: item.name,
          createTime,
          status,
          sendNumber,
          parseNumber,
          readDataCount,
          readerNumber,
          readerkbNumber,
          readerTrend,
          readerkbTrend,
          sendTrend,
          parseTrend,
          parseSuccessOfTotal: parseSuccessNumber + '/' + (parseSuccessNumber + parseFailNumber),
          successOfTotal: successNumber + '/' + (successNumber + failNUmber),
          path: logpath,
          readerError,
          parseError,
          sendError,
          logkitError,
          copy: JSON.stringify(item, null, 2),
          currentItem: item,
          iconType,
          runnerOpt,
          isWebFolder
        })
      })
    }

    return (
      <Table columns={columns} pagination={{size: 'small', pageSize: 20}} dataSource={data}/>
    )
  }

  addLogRunner = () => {
    this.props.router.push({pathname: `/index/create-log-runner`})
  }

  addMetricRunner = () => {
    this.props.router.push({pathname: `/index/create-metric-runner`})
  }

  changeMenu = (e) => {
    //console.log(e)
    this.setState({
      currentMenu: e.key
    })
  }

  render() {
    return (
        <Layout style={{ minHeight: '100vh' }}>
          <Sider
              collapsible
              collapsed={this.state.collapsed}
              onCollapse={this.onCollapse}
          >
            <div className="logo" >  <img src="../../../static/logkit100.png"></img> </div>
            <Menu theme="dark" defaultSelectedKeys={['1']} mode="inline" onClick={this.changeMenu}>
              <Menu.Item key="tag">
                <Icon type="pie-chart" />
                <span>标签</span>
              </Menu.Item>
              <Menu.Item key="machine">
                <Icon type="desktop" />
                <span>机器</span>
              </Menu.Item>
              <Menu.Item key="runner">
                <Icon type="file" />
                <span>Runner</span>
              </Menu.Item>
            </Menu>
          </Sider>
          <Layout>
            <Header style={{ background: '#fff', padding: 0 }} />
            {this.state.currentMenu === 'tag' ? (<Content style={{ margin: '0 16px' }}>
              <Breadcrumb style={{ margin: '16px 0' }}>
                <Breadcrumb.Item>标签管理列表</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{ padding: 24, background: '#fff', minHeight: 360 }}>
                <div className="content">
                  {this.renderTagList()}
                </div>

              </div>
            </Content>) : null}
            {this.state.currentMenu === 'machine' ? (<Content style={{ margin: '0 16px' }}>
              <Breadcrumb style={{ margin: '16px 0' }}>
                <Breadcrumb.Item>机器管理列表</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{ padding: 24, background: '#fff', minHeight: 360 }}>
                <div className="content">
                  {this.renderMachineList()}
                </div>

              </div>
            </Content>) : null}
            {this.state.currentMenu === 'runner' ? (<Content style={{ margin: '0 16px' }}>
              <Breadcrumb style={{ margin: '16px 0' }}>
                <Breadcrumb.Item>Runner管理列表</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{ padding: 24, background: '#fff', minHeight: 360 }}>
                <div className="content">
                  {this.renderRunnerList()}
                </div>

              </div>
            </Content>) : null}
            <Footer style={{textAlign: 'center'}}>
              更多信息请访问：
              <a target="_blank" href="https://github.com/qiniu/logkit">
              <Tag color="#108ee9">Logkit</Tag> </a> |
              <a target="_blank" href="https://github.com/qiniu/logkit/wiki">
              <Tag color="#108ee9">帮助文档</Tag> </a> |
              <a target="_blank" href="https://qiniu.github.io/pandora-docs/#/"><Tag
                   color="#108ee9">Pandora产品</Tag>
                 </a>
            </Footer>
          </Layout>
          <Modal footer={null} title="错误日志" width={1000} visible={this.state.isShow}
                 onCancel={this.handleErrorCancel}
          >
            <Tag color="#108ee9">读取错误日志:</Tag>
            <Row style={{marginTop: '10px'}}>
              <Col span={24}>{this.state.currentItem.readerError}</Col>
            </Row>
            <Tag color="#108ee9" style={{marginTop: '30px'}}>解析错误日志:</Tag>
            <Row style={{marginTop: '10px'}}>
              <Col span={24}>{this.state.currentItem.parseError}</Col>
            </Row>
            <Tag color="#108ee9" style={{marginTop: '30px'}}>发送错误日志:</Tag>
            <Row style={{marginTop: '10px'}}>
              <Col span={24}>{this.state.currentItem.sendError}</Col>
            </Row>
            <Tag color="#108ee9" style={{marginTop: '30px'}}>Logkit错误日志:</Tag>
            <Row style={{marginTop: '10px'}}>
              <Col span={24}>{this.state.currentItem.logkitError}</Col>
            </Row>

          </Modal>
          <Modal footer={null} title="详细配置情况" width={1000} visible={this.state.isShowConfig}
                 onCancel={this.handleConfigCancel}
          >
            <Input type="textarea" value={this.state.currentItem.copy} rows="50"/>

          </Modal>

          <Modal title="是否重置配置文件？" visible={this.state.isShowResetConfig}
                 onOk={this.handleResetConfig} onCancel={this.handleResetConfigCancel}
          >
            注意:<Tag color="#ffbf00">重置配置文件会删除meta信息并重启</Tag>
          </Modal>
        </Layout>

    );
  }
}
export default List;