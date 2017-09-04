import React, {Component} from 'react';
import {Table, Icon, Popconfirm, Button, notification, Modal, Row, Col, Tag} from 'antd';
import ClipboardButton from 'react-clipboard.js';
import {
  getRunnerConfigs, deleteConfigData, getRunnerStatus
} from './services/logkit';
import _ from "lodash";

class List extends Component {
  constructor(props) {
    super(props);
    this.state = {
      runners: [],
      status: [],
      isShow: false,
      currentError: ''
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
    that.getStatus()

    if (window.statusInterval != undefined && window.statusInterval != 'undefined') {
      window.clearInterval(window.statusInterval);
    }
    window.statusInterval = setInterval(function () {
      that.getStatus()
    }, 10000)

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

  isShow = (currentError) => {
    this.setState({
      currentError: currentError,
      isShow: true
    })
  }

  handleErrorCancel = () => {
    this.setState({
      isShow: false
    })
  }

  renderRunnerList() {
    const columns = [{
      title: '名称',
      dataIndex: 'name',
      width: '10%'
    }, {
      title: '运行状态',
      dataIndex: 'status',
      width: '8%'
    }, {
      title: '解析速率(kb/s)',
      dataIndex: 'parseNumber',
      width: '8%'
    }, {
      title: '发送速率(kb/s)',
      dataIndex: 'sendNumber',
      width: '8%'
    }, {
      title: '解析成功条数',
      dataIndex: 'parseSuccessNumber',
      width: '8%'
    }, {
      title: '解析失败条数',
      dataIndex: 'parseFailNUmber',
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
      title: '点击图标复制配置文件',
      key: 'copy',
      dataIndex: 'copy',
      width: '6%',
      render: (text, record) => {
        return (
            <a>
              <div className="editable-row-operations">
                <ClipboardButton data-clipboard-text={text}>
                  <Icon style={{fontSize: '15px'}} type="copy"/>
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
            record.isWebFolder == true ? (<a>
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
      _.values(this.state.runners).map((item, value) => {
        let status = '异常'
        let parseSuccessNumber = 0
        let parseFailNUmber = 0
        let successNumber = 0
        let failNUmber = 0
        let logpath = ''
        let sendNumber = 0
        let parseNumber = 0
        let isWebFolder = false
        let errorLog = {
          parseError: '',
          sendError: '',
        }

        this.state.status.map((ele) => {
          if (item.name == ele.name) {
            status = '正常'
            parseSuccessNumber = ele.parserStats.success
            parseFailNUmber = ele.parserStats.errors
            successNumber = _.values(ele.senderStats)[0].success
            failNUmber = _.values(ele.senderStats)[0].errors
            sendNumber = _.floor(_.values(ele.senderStats)[0].success / ele.elaspedtime, 3)
            parseNumber = _.floor(ele.readDataCount / ele.elaspedtime, 3)
            logpath = ele.logpath
            errorLog.parseError = ele.parserStats.last_error
            errorLog.sendError = _.values(ele.senderStats)[0].last_error
            isWebFolder = item.web_folder
          }
        })
        data.push({
          key: item.name,
          name: item.name,
          status: status,
          sendNumber: sendNumber,
          parseNumber: parseNumber,
          parseSuccessNumber: parseSuccessNumber,
          parseFailNUmber: parseFailNUmber,
          successNumber: successNumber,
          failNUmber: failNUmber,
          path: logpath,
          errorLog: errorLog,
          copy: JSON.stringify(item, null, 2),
          isWebFolder: isWebFolder
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
            七牛Logkit配置文件助手
          </div>
          <div className="content">
            <Button type="primary" className="index-btn" ghost onClick={this.add}>
              <Icon type="plus"/> 增加Runner
            </Button>
            <Button type="primary" className="index-btn" ghost onClick={() => this.turnToConfigPage()}>
              <Icon type="link"/>跳转至配置页面
            </Button>
            {this.renderRunnerList()}
          </div>
          <Modal footer={null} title="错误日志" width="1000" visible={this.state.isShow}
                 onCancel={this.handleErrorCancel}
          >
            <Tag color="#108ee9">解析错误日志:</Tag>
            <Row>
              <Col span={24}>{this.state.currentError.parseError}</Col>
            </Row>
            <Tag color="#108ee9" style={{marginTop: '30px'}}>发送错误日志:</Tag>
            <Row>
              <Col span={24}>{this.state.currentError.sendError}</Col>
            </Row>

          </Modal>
        </div>
    );
  }
}
export default List;