import React, {Component} from 'react';
import moment from 'moment'
import ClipboardButton from 'react-clipboard.js';
import {
  Table,
  Icon,
  Popconfirm,
  Button,
  notification,
  Modal,
  Row,
  Col,
  Tag,
  Input,
  Layout,
  Select,
  Breadcrumb,
  Form
} from 'antd';
import {
  deleteConfigData,
  resetConfigData,
  startRunner,
  stopRunner
} from '../../services/logkit';
import config from '../../store/config'
import * as uuid from 'uuid'
import _ from "lodash";

const FormItem = Form.Item;
const Option = Select.Option
const formItemLayout = {
  labelCol: {span: 7},
  wrapperCol: {span: 17},
}
class RunnerTable extends Component {
  constructor(props) {
    super(props);
    this.state = {
      current: 0,
      currentItem: '',
      status: [],
      runners: [],
      machineUrl: '',
      tag: ''
    };
  }

  componentDidMount() {

  }

  componentWillUnmount() {
  }

  componentDidUpdate(prevProps) {

  }

  deleteRunner = (record) => {
    deleteConfigData({name: record.name}).then(data => {
      if (!data) {
        notification.success({message: "删除成功", duration: 10,})
        this.init()
      }
    })
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
      if (!data) {
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

  setMachine = (machineUrl) => {
    const {searchRunner} = this.props
    // this.setState({
    //   machineUrl: machineUrl
    // })
    window.machine_url = machineUrl
    searchRunner()
  }

  setTag = (tag) => {
    console.log(tag)
    const {searchRunner} = this.props
    // this.setState({
    //   tag: tag
    // })
    window.tag = tag
    searchRunner()
  }

  copyConfig = (record) => {
    window.nodeCopy = record
    notification.success({message: "修改配置文件,", description: '按步骤去修改配置页面的Runner信息', duration: 10,})
    if (record["metric"] === undefined) {
      this.props.router.push({pathname: `/index/create-log-runner?copyConfig=true`})
    } else {
      this.props.router.push({pathname: `/index/create-metric-runner?copyConfig=true`})
    }
  }

  optRunner = (record) => {
    if (record.iconType === 'caret-right') {
      startRunner({name: record.name}).then(data => {
        if (!data) {
          record.iconType = 'poweroff'
          notification.success({message: '开启成功', duration: 10})
        }
      })
    } else {
      stopRunner({name: record.name}).then(data => {
        if (!data) {
          record.iconType = 'caret-right'
          notification.success({message: '关闭成功', duration: 10})
        }
      })
    }
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


  renderRunnerList() {
    const {runners, runnerStatus} = this.props

    let runnerList = []
    let statusList = []
    let tagList = []
    let machineList = []
    runners.map(item => {
      if (item.configs.length > 0) {
        item.configs.map(runner => {
          runnerList.push(runner)
        })
      }
      if (!_.includes(tagList, item.tag)) {
        tagList.push(item.tag)
      }

    })

    runnerStatus.map(item => {
      if (item.runnerStatus.length > 0) {
        item.runnerStatus.map(statu => {
          statusList.push(statu)
        })
      }
      machineList.push(item.machineUrl)
    })

    const columns = [{
      title: '名称',
      dataIndex: 'name',
      width: '10%'
    }, {
      title: '标签',
      dataIndex: 'tag',
      width: '5%'
    }, {
      title: '机器地址',
      dataIndex: 'machineUrl',
      width: '5%'
    }, {
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
                  <Popconfirm title={"是否" + record.runnerOpt + "该Runner?"} onConfirm={() => this.optRunner(record)}>
                    <Icon title={record.runnerOpt + "Runner"} style={{fontSize: 16}} type={record.iconType}/>
                  </Popconfirm>
                }
              </div>
            </a>) : null
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
            </a>) : null
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
    if (runnerList != null) {
      runnerList.map((item) => {
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
        let tag = ''
        let machineUrl = ''

        if (item["is_stopped"]) {
          status = '关闭'
          runnerOpt = '开启'
          iconType = 'caret-right'
          isWebFolder = item.web_folder
        }
        statusList.map((ele) => {
          if (item.name === ele.name) {
            status = '正常'
            tag = item.tag
            machineUrl = item.machineUrl
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
          key: uuid(),
          name: item.name,
          tag,
          machineUrl,
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

    return (<div>
          <Form layout="inline">
            <FormItem {...formItemLayout} style={{width: '300px'}} label="标签名称">
              <Select className="select-field"
                      showSearch
                      defaultValue=''
                      onChange={(val) => this.setTag(val)}
              ><Option
                  key={'alltag'}
                  value={''}>{'全部'}</Option>
                {
                  tagList.map(
                      tag => <Option
                          key={tag}
                          value={tag}>{tag}</Option>
                  )
                }
              </Select>
            </FormItem>
            <FormItem {...formItemLayout} style={{width: '300px'}} label="机器地址">
              <Select className="select-field"
                      showSearch
                      defaultValue=''
                      onChange={(val) => this.setMachine(val)}
              ><Option
                  key={'allmachine'}
                  value={''}>{'全部'}</Option>
                {
                  machineList.map(
                      machineUrl => <Option
                          key={machineUrl}
                          value={machineUrl}>{machineUrl}</Option>
                  )
                }
              </Select>
            </FormItem>
          </Form>
          <Table columns={columns} pagination={{size: 'small', pageSize: 20}} dataSource={data}/></div>
    )
  }

  render() {
    return (
        <div>
          {this.renderRunnerList()}
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
        </div>
    );
  }
}
export default Form.create()(RunnerTable);