import React, {Component} from 'react';
import moment from 'moment'
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
  Select,
  Form,
  AutoComplete
} from 'antd';
import {
  deleteConfigData,
  resetConfigData,
  startRunner,
  stopRunner,
  deleteClusterConfigData,
  startClusterRunner,
  stopClusterRunner,
  resetClusterConfigData,
  getRunnerConfigs,
  getClusterRunnerConfigs,
  getRunnerStatus,
  getClusterRunnerStatus,
} from '../../services/logkit';
import {runnerStatu} from './constant'
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
      runnerStatus: [],
      machineUrl: '',
      tag: '',
      isLoading: false,
      currentTag: '',
      currentUrl: '',
      data: [],
      isFiltered: false,
      isGetStatus: true
    };

  }

  componentDidMount() {
    this.init()
  }

  componentWillUnmount() {

  }

  componentWillReceiveProps(nextProps) {
    this.getStatus()
  }

  componentDidUpdate(prevProps) {

  }

  transformRunner = (srcData) => {
    let dataArray = []
    _.forIn(srcData, (value, key) => {
      let runners = [];
      if (value.configs != null) {

        _.forIn(value.configs, (v, k) => {
          _.merge(v, {tag: value.tag, machineUrl: key});
          runners.push(v);
        })
      }
      dataArray.push({
        machineUrl: key,
        configs: runners,
        error: value.error,
        tag: value.tag
      })

    })
    return dataArray
  }

  transformStatus = (srcData) => {
    let dataArray = []
    _.forIn(srcData, (value, key) => {
      let runnerStatus = [];
      if (value.status !== null) {
        _.forIn(value.status, (v, k) => {
          _.merge(v, {tag: value.tag, machineUrl: key});
          runnerStatus.push(v);
        })
      }
      dataArray.push({
        machineUrl: key,
        runnerStatus: runnerStatus,
        error: value.error,
        tag: value.tag
      })

    })
    return dataArray
  }


  getStatus = (tag, url) => {
    let that = this
    that.setState({
      isGetStatus: true
    })
    if (window.isCluster && window.isCluster === true) {
      getClusterRunnerConfigs({
        tag: tag ? tag : this.state.currentTag,
        machineUrl: url ? url : this.state.currentUrl
      }).then(data => {
        if (data.code === 'L200') {
          let mapData = data.data
          let tagMapData = this.transformRunner(mapData)
          that.setState({
            runners: tagMapData
          })
          getClusterRunnerStatus({
            tag: tag ? tag : this.state.currentTag,
            machineUrl: url ? url : this.state.currentUrl
          }).then(data => {
            if (data.code === 'L200') {
              that.setState({
                runnerStatus: this.transformStatus(data.data),
                isLoading: false,
                isGetStatus: false
              })
            }
          })
        }
      })
    } else {
      getRunnerConfigs().then(item => {
        if (item.code === 'L200') {
          that.setState({
            runners: _.values(item.data)
          })
          getRunnerStatus().then(item => {
            if (item.code === 'L200') {
              that.setState({
                runnerStatus: _.values(item.data),
                isLoading: false,
                isGetStatus: false
              })
            }
          })
        }
      })
    }
  }

  init = () => {
    let that = this
    that.getStatus()

    if (window.statusInterval !== undefined && window.statusInterval !== 'undefined') {
      window.clearInterval(window.statusInterval);
    }

    window.statusInterval = setInterval(function () {
      if (that.state.isGetStatus === false) {
        that.getStatus()
      }
    }, 8000)

  }

  deleteRunner = (record) => {
    this.setState({
      isLoading: true
    })
    if (window.isCluster === true) {
      deleteClusterConfigData({name: record.name, tag: record.tag, url: record.machineUrl}).then(item => {
        if (item.code === 'L200') {
          notification.success({message: "删除成功", duration: 10,})
          this.getStatus()
        }
      })
    } else {
      deleteConfigData({name: record.name}).then(item => {
        if (item.code === 'L200') {
          notification.success({message: "删除成功", duration: 10,})
          this.getStatus()
        }
      })
    }

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

  handleResetConfig = (record) => {
    this.setState({
      isLoading: true
    })
    if (window.isCluster === true) {
      resetClusterConfigData({name: record.name, tag: record.tag, url: record.machineUrl}).then(item => {
        if (item.code === 'L200') {
          notification.success({message: "重置成功", duration: 10,})
        }
        this.getStatus()
      })
    } else {
      resetConfigData({name: record.name}).then(item => {
        if (item.code === 'L200') {
          notification.success({message: "重置成功", duration: 10,})
        }
        this.getStatus()
      })
    }
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
    this.setState({
      currentUrl: machineUrl
    })
    this.getStatus('',machineUrl)
  }

  setTag = (tag) => {

    this.setState({
      currentTag: tag
    })
    this.getStatus(tag,'')
  }

  copyConfig = (record) => {
    const {handleTurnToRunner, handleTurnToMetricRunner} = this.props
    window.nodeCopy = record
    notification.success({message: "修改配置文件,", description: '按步骤去修改配置页面的Runner信息', duration: 10,})
    if (record["metric"] === undefined) {
      handleTurnToRunner()
    } else {
      handleTurnToMetricRunner()
    }
  }

  optRunner = (record) => {
    this.setState({
      isLoading: true
    })
    if (record.iconType === 'caret-right') {
      if (window.isCluster === true) {
        startClusterRunner({name: record.name, tag: record.tag, url: record.machineUrl}).then(item => {
          if (item.code === 'L200') {
            record.iconType = 'poweroff'
            notification.success({message: '开启成功', duration: 10})
            this.getStatus()
          }
        })
      } else {
        startRunner({name: record.name}).then(item => {
          if (item.code === 'L200') {
            record.iconType = 'poweroff'
            notification.success({message: '开启成功', duration: 10})
            this.getStatus()
          }
        })
      }
    } else {
      if (window.isCluster === true) {
        stopClusterRunner({name: record.name, tag: record.tag, url: record.machineUrl}).then(item => {
          if (item.code === 'L200') {
            record.iconType = 'caret-right'
            notification.success({message: '关闭成功', duration: 10})
            this.getStatus()
          }
        })
      } else {
        stopRunner({name: record.name}).then(item => {
          if (item.code === 'L200') {
            record.iconType = 'caret-right'
            notification.success({message: '关闭成功', duration: 10})
            this.getStatus()
          }
        })
      }
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

    const {handleAddRunner, handleAddMetricRunner} = this.props
    let runnerList = []
    let statusList = []
    let tagList = []
    let machineList = []
    if (window.isCluster && window.isCluster === true) {
      this.state.runners.map(item => {
        if (item.configs && item.configs.length > 0) {
          item.configs.map(runner => {
            runnerList.push(runner)
          })
        }
        if (!_.includes(tagList, item.tag)) {
          tagList.push(item.tag)
        }

      })

      this.state.runnerStatus.map(item => {
        if (item.runnerStatus && item.runnerStatus.length > 0) {
          item.runnerStatus.map(statu => {
            statusList.push(statu)
          })
        }
        machineList.push(item.machineUrl)
      })
    } else {
      runnerList = this.state.runners
      statusList = this.state.runnerStatus
    }

    const columns = [{
      title: '名称',
      dataIndex: 'name',
      width: '10%'
    }, {
      title: '集群',
      dataIndex: 'tag',
      width: '5%',
      className: window.isCluster === true ? '' : 'hide-div'
    }, {
      title: '机器地址',
      dataIndex: 'machineUrl',
      width: '10%',
      className: window.isCluster === true ? '' : 'hide-div'
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
      width: '6%',
      render: (text, record) => {
        return (
            this.renderArrow(text, record.readerTrend)
        );
      },
    }, {
      title: '读取速率(KB/s)',
      dataIndex: 'readerkbNumber',
      width: '6%',
      render: (text, record) => {
        return (
            this.renderArrow(text, record.readerkbTrend)
        );
      },
    }, {
      title: '发送速率(条/s)',
      dataIndex: 'sendNumber',
      width: '6%',
      render: (text, record) => {
        return (
            this.renderArrow(text, record.sendTrend)
        );
      },
    }, {
      title: '解析成功/总 (条数)',
      dataIndex: 'parseSuccessOfTotal',
      width: '6%'
    }, {
      title: '发送成功/总 (条数)',
      dataIndex: 'successOfTotal',
      width: '6%'
    }, {
      title: '路径',
      dataIndex: 'path',
      width: '8%'
    }, {
      title: '待读取数据',
      dataIndex: 'lag',
      render: (text, record, all) => {
        if(text){
          return <span>{(text.size||0) + (text.sizeunit ? ' ' + text.sizeunit : text.sizeunit)}</span>
        }
        return null
      }
    }, {
      title: '错误日志',
      dataIndex: 'errorLog',
      width: '3%',
      render: (text, record) => {
        return (
            <a>
              <div className="editable-row-operations">
                {
                  <Icon title="查看错误日志" style={{fontSize: 16}} type="eye-o" onClick={() => this.isShow(record)} />
                }
              </div>
            </a>
        );
      },

    }, {
      title: '详细配置',
      dataIndex: 'config',
      width: '3%',
      render: (text, record) => {
        return (
            <a>
              <div className="editable-row-operations">
                {
                  <Icon title="查看收集器配置" style={{fontSize: 16}} type="eye-o" onClick={() => this.showConfig(record)} />
                }
              </div>
            </a>
        );
      },
    }, {
      title: '编辑',
      key: 'copy',
      dataIndex: 'copy',
      width: '4%',
      render: (text, record) => {
        return (
            <a>
              <div className="editable-row-operations">
                <Icon title={"编辑Runner"} style={{fontSize: 16}} onClick={() => this.copyConfig(record.currentItem)} type="edit"/>
              </div>
            </a>
        );
      },
    }, {
      title: '操作',
      key: 'opt',
      dataIndex: 'opt',
      width: '4%',
      render: (text, record) => {
        return (
            record.isWebFolder === true ? (<a>
              <div className="editable-row-operations">
                {
                  <Popconfirm title={"是否" + record.runnerOpt + "该收集器?"} onConfirm={() => this.optRunner(record)}>
                    <Icon title={record.runnerOpt + "收集器"} style={{fontSize: 16}} type={record.iconType}/>
                  </Popconfirm>
                }
              </div>
            </a>) : null
        );
      },
    }, {
      title: '重置',
      dataIndex: 'reset',
      width: '4%',
      render: (text, record) => {
        return (
            record.isWebFolder === true ? (<a>
              <div className="editable-row-operations">
                {
                  <Popconfirm title={"重置配置文件会删除meta信息并重启,是否重置该收集器?"}
                              onConfirm={() => this.handleResetConfig(record)}>
                    <Icon title={"重置收集器"} style={{fontSize: 16}} type='reload'/>
                  </Popconfirm>
                }
              </div>
            </a>) : null
        );
      },
    }, {
      title: '删除',
      key: 'edit',
      dataIndex: 'edit',
      width: '4%',
      render: (text, record) => {
        return (
            record.isWebFolder === true ? (<a>
              <div className="editable-row-operations">
                {
                  <Popconfirm title="是否删除该收集器?" onConfirm={() => this.deleteRunner(record)}>
                    <Icon title={"删除收集器"} style={{fontSize: 16}} type="delete"/>
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
        let status = ''
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
        let runnerOpt = '停止'
        let tag = ''
        let machineUrl = ''
        let lag = {}

        if (item["is_stopped"]) {
          status = '关闭'
          runnerOpt = '重启'
          iconType = 'caret-right'
          isWebFolder = item.web_folder
        }
        statusList.map((ele) => {
          if ((item.name + item.machineUrl) === (ele.name + ele.url)) {
            status = runnerStatu[ele.runningStatus]
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
            lag = ele.lag
          }
        })
        data.push({
          key: uuid(),
          name: item.name,
          tag,
          lag,
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
          {window.isCluster === true ? (<Form layout="inline">
            <FormItem {...formItemLayout} style={{width: '300px'}} label="集群名称">
              <Select className="select-field"
                      showSearch
                      defaultValue=''
                      onChange={(val) => this.setTag(val)}
              ><Option
                  key={'alltag'}
                  value={''}>{'全部'}</Option>
                {
                  tagList.map(
                      (tag,i) => <Option
                          key={tag+i}
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
                      (machineUrl,i) => <Option
                          key={machineUrl+i}
                          value={machineUrl}>{machineUrl}</Option>
                  )
                }
              </Select>
            </FormItem>
          </Form>)
            : (<div style={{marginBottom: '15px'}}>
                <Button type="primary" style={{marginRight: '50px'}} className="index-btn" ghost onClick={handleAddRunner}>
                  <Icon type="plus"/> 增加日志采集收集器
                </Button>
                <Button type="primary" className="index-btn" ghost onClick={handleAddMetricRunner}>
                  <Icon type="plus"/> 增加系统信息采集收集器
                </Button>
              <Input suffix={<Icon type="search" className="certain-category-icon" />}
                     onChange={(e) => {
                       this.setState({
                         data: data.filter((item) => item.name.indexOf(e.target.value) > -1),
                         isFiltered: true
                       })
                     }}
                     placeholder="在此输入名称"
                      style={{width: 200, float: 'right'}}/>
              </div>)}
              {this.state.isFiltered
                ? <Table columns={columns} pagination={{size: 'small', pageSize: 20}} dataSource={this.state.data} />
                : <Table columns={columns} pagination={{size: 'small', pageSize: 20}} dataSource={data} />
              }

          </div>
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
        </div>
    )
  }
}
export default Form.create()(RunnerTable);