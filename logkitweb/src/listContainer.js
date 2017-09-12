import React, {Component} from 'react';
import {Table, Icon, Popconfirm, Button, notification, Modal, Row, Col, Tag, Input} from 'antd';
import ClipboardButton from 'react-clipboard.js';
import {
  getRunnerConfigs, deleteConfigData, getRunnerStatus, getRunnerVersion, resetConfigData
} from './services/logkit';
import _ from "lodash";

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
      version: ''
    };
    this.init()
  }

  componentDidMount() {

  }

  componentWillUnmount() {

  }

  componentDidUpdate(prevProps) {

  }

  getStatus = () => {
    let that = this
    getRunnerConfigs().then(data => {
      if (data.success) {
        that.setState({
          runners: _.omit(data, 'success')
        })
        getRunnerStatus().then(data => {
          if (data.success) {
            that.setState({
              status: _.values(_.omit(data, 'success'))
            })
          }
        })
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

  deleteRunner = (record) => {
    deleteConfigData({name: record.name}).then(data => {
      notification.success({message: "删除成功", duration: 10,})
      this.init()
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
      notification.success({message: "重置成功", duration: 10,})
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
    notification.success({message: "复制配置文件成功,", description: '配置页面可修改Runner的配置信息', duration: 10,})
    this.props.router.push({pathname: `/index/create?copyConfig=true`})
  }

  renderRunnerList() {
    const columns = [{
      title: '名称',
      dataIndex: 'name',
      width: '10%'
    }, {
      title: '运行状态',
      dataIndex: 'status',
      width: '5%'
    }, {
      title: '读取速率(KB/s)',
      dataIndex: 'readerNumber',
      width: '7%',
      render: (text, record) => {
        return (
            this.renderArrow(text, record.readerTrend)
        );
      },
    }, {
      title: '解析速率(KB/s)',
      dataIndex: 'parseNumber',
      width: '7%',
      render: (text, record) => {
        return (
            this.renderArrow(text, record.parseTrend)
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
      title: '解析成功条数',
      dataIndex: 'parseSuccessNumber',
      width: '8%'
    }, {
      title: '解析失败条数',
      dataIndex: 'parseFailNumber',
      width: '8%'
    }, {
      title: '发送成功条数',
      dataIndex: 'successNumber',
      width: '8%'
    }, {
      title: '发送失败条数',
      dataIndex: 'failNUmber',
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
      title: '重置配置',
      dataIndex: 'reset',
      width: '3%',
      render: (text, record) => {
        return (
            <a>
              <div className="editable-row-operations">
                {
                  <Button type="primary" onClick={() => this.showResetConfig(record)}>重置</Button>
                }
              </div>
            </a>
        );
      },
    }, {
      title: '复制配置并跳转至配置页面',
      key: 'copy',
      dataIndex: 'copy',
      width: '6%',
      render: (text, record) => {
        return (
            <a>
              <div className="editable-row-operations">
                <ClipboardButton data-clipboard-text={text}>
                  <Icon style={{fontSize: '15px'}} onClick={() => this.copyConfig(text)} type="copy"/>
                </ClipboardButton>
              </div>
            </a>
        );
      },

    }, {
      title: '删除',
      key: 'edit',
      dataIndex: 'edit',
      width: '6%',
      render: (text, record) => {
        return (
            record.isWebFolder === true ? (<a>
              <div className="editable-row-operations">
                {
                  <Popconfirm title="是否删除该Runner?" onConfirm={() => this.deleteRunner(record)}>
                    <Icon style={{fontSize: '15px'}} type="delete"/>
                  </Popconfirm>
                }
              </div>
            </a>) : null
        );
      },

    }];

    let data = []
    if (this.state.runners != null) {
      _.values(this.state.runners).map((item) => {
        let status = '异常'
        let parseSuccessNumber = 0
        let parseFailNumber = 0
        let successNumber = 0
        let readerNumber = 0
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

        this.state.status.map((ele) => {
          if (item.name === ele.name) {
            status = '正常'
            parseSuccessNumber = ele.parserStats.success
            parseFailNumber = ele.parserStats.errors
            successNumber = _.values(ele.senderStats)[0].success
            failNUmber = _.values(ele.senderStats)[0].errors
            sendNumber = _.floor(_.values(ele.senderStats)[0].speed, 3)
            parseNumber = _.floor(ele.parserStats.speed, 3)
            readerNumber = _.floor(ele.readerStats.speed, 3)
            sendTrend = _.values(ele.senderStats)[0].trend
            parseTrend = ele.parserStats.trend
            readerTrend = ele.readerStats.trend
            logpath = ele.logpath
            readerError = ele.readerStats.last_error
            parseError = ele.parserStats.last_error
            sendError = _.values(ele.senderStats)[0].last_error
            isWebFolder = item.web_folder
            logkitError = ele.error
          }
        })
        data.push({
          key: item.name,
          name: item.name,
          status,
          sendNumber,
          parseNumber,
          readerNumber,
          readerTrend,
          sendTrend,
          parseTrend,
          parseSuccessNumber,
          parseFailNumber,
          successNumber,
          failNUmber,
          path: logpath,
          readerError,
          parseError,
          sendError,
          logkitError,
          copy: JSON.stringify(item, null, 2),
          isWebFolder
        })
      })

    }

    return (
        <Table columns={columns} pagination={{size: 'small', pageSize: 5}} dataSource={data}/>
    )
  }

  add = () => {
    this.props.router.push({pathname: `/index/create`})
  }

  render() {
    return (
        <div className="logkit-container">
          <div className="header">
            七牛Logkit配置文件助手- {this.state.version}
          </div>
          <div className="content">
            <Button type="primary" className="index-btn" ghost onClick={this.add}>
              <Icon type="plus"/> 增加Runner
            </Button>
            {/*<Button type="primary" className="index-btn" ghost onClick={() => this.turnToConfigPage()}>*/}
              {/*<Icon type="link"/>跳转至配置页面*/}
            {/*</Button>*/}
            {this.renderRunnerList()}
          </div>
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
            注意:<Tag color="#108ee9">重置配置文件会删除meta信息并重启</Tag>
          </Modal>
        </div>
    );
  }
}
export default List;