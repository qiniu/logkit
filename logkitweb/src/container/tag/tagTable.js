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
  Menu,
  Breadcrumb,
  Form
} from 'antd';
import {
  getRunnerConfigs,
  deleteClusterSlaveTag,
  postClusterStopSlaveTag,
  postClusterStartSlaveTag,
  getClusterRunnerConfigs,
  postClusterSlaveTag,
  deleteConfigData,
  getClusterSlaves,
  getRunnerStatus,
  getClusterRunnerStatus,
  getRunnerVersion,
  resetConfigData,
  startRunner,
  stopRunner
} from '../../services/logkit';
import {titles} from './constant'
import config from '../../store/config'
import _ from "lodash";
const FormItem = Form.Item;

class TagTable extends Component {
  constructor(props) {
    super(props);
    this.state = {
      status: [],
      runners: [],
      machines: [],
      tags: [],
      isShowTagModal: false,
      isShowDeleteTag: false,
      currentTag: '',
      currentTagName: '',
      currentModalType: '',
      currentRunnerName: ''
    };
  }

  componentDidMount() {
    this.init()
  }

  componentWillUnmount() {
  }

  componentDidUpdate(prevProps) {

  }

  init() {
    getClusterSlaves().then(data => {
      if (data.code === 'L200') {
        console.log(data)
        this.setState({
          machines: _.values(data.data)
        })
      }
    })
  }

  showTagModal = (item, type) => {
    console.log(item)
    this.setState({
      currentTag: item,
      isShowTagModal: true,
      currentModalType: type,
      currentModalTitle: titles[type]
    })
  }

  showDeleteTag = (item) => {
    this.setState({
      currentTag: item,
      isShowDeleteTag: true,
    })
  }

  handleTagModal = () => {
    if (this.state.currentModalType == 'rename') {
      postClusterSlaveTag({
        name: this.state.currentTag.name,
        url: '',
        body: {tag: this.state.currentTagName}
      }).then(data => {
        notification.success({message: "重置成功", duration: 10,})
        this.setState({
          isShowTagModal: false
        })
      })
    } else if (this.state.currentModalType == 'stop') {
      postClusterStopSlaveTag({
        name: this.state.currentRunnerName,
        tag: this.state.currentTag.name,
        url: ''
      }).then(data => {
        if (!data) {
          notification.success({message: '关闭成功', duration: 10})
        }
      })
    } else if (this.state.currentModalType == 'start') {
      postClusterStartSlaveTag({
        name: this.state.currentRunnerName,
        tag: this.state.currentTag.name,
        url: ''
      }).then(data => {
        if (!data) {
          notification.success({message: '开启成功', duration: 10})
        }
      })
    }
  }

  handleDeleteTag = () => {
    deleteClusterSlaveTag({name: this.state.currentTag.name, url: ''}).then(data => {
      notification.success({message: "重置成功", duration: 10,})
      this.setState({
        isShowDeleteTag: false
      })
    })
  }

  handleTagModalCancel = () => {
    this.setState({
      isShowTagModal: false
    })
  }

  handleDeleteTagCancel = () => {
    this.setState({
      isShowDeleteTag: false
    })
  }

  changeTagName = (e) => {
    console.log(e.target.value)
    this.setState({
      currentTagName: e.target.value
    })
  }

  changeRunnerName = (e) => {
    console.log(e.target.value)
    this.setState({
      currentRunnerName: e.target.value
    })
  }

  printString = (strs) => {
    let prints = ''
    strs.map((item, i) => {
      prints += item + '   '
    })
    return prints
  }

  checkStatus = (status) => {
    if (_.includes(status,'lost')) {
      return 'lost'
    }
    else if (_.includes(status,'bad')) {
      return 'bad'
    }
    else {
      return 'ok'
    }
  }


  renderTagList() {
    let dataSource = []
    const {tags, handleAddRunner, handleAddMetricRunner} = this.props
    let machines = this.state.machines
    let currentItem = null
    let count = 0
    if (machines.length > 0) {
      currentItem = _.sortBy(machines, 'tag')[0]
    }
    let machineUrl = []
    let status = []
    _.sortBy(machines, 'tag').map((item, i) => {
      console.log(i)
      if (item.tag === currentItem.tag) {
        machineUrl.push(item.url)
        status.push(item.status);
        if (i === machines.length - 1) {
          dataSource.push({
            name: currentItem.tag,
            machineUrl: machineUrl,
            status: status,
          })
        }
      }
      else if (item.tag !== currentItem.tag) {
        console.log(currentItem)
        dataSource.push({
          name: currentItem.tag,
          machineUrl: machineUrl,
          status: status,
        })
        if (i === machines.length - 1) {
          dataSource.push({
            name: item.tag,
            machineUrl: machineUrl,
            status: status,
          })
        }
        currentItem = item;
        count = 0;
        machineUrl = [item.url]
        status = [item.status]
      }

    })

    const columns = [{
      title: '标签名称',
      dataIndex: 'name',
      key: 'name',
      width: '10%',
    }, {
      title: '机器地址',
      dataIndex: 'machineUrl',
      key: 'machineUrl',
      render: (text, record) => {
        return (
            (
                <div className="editable-row-operations">
                  {
                    this.printString(record.machineUrl)
                  }
                </div>
            )
        );
      }
    }, {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (text, record) => {
        return (
            (
                <div className="editable-row-operations">
                  {
                    this.checkStatus(record.status)
                  }
                </div>
            )
        );
      }
    }, {
      title: '重命名',
      dataIndex: 'rename',
      key: 'rename',
      width: '10%',
      render: (text, record) => {
        return (
            (
            <a>
              <div className="editable-row-operations">
                { this.checkStatus(record.status) === 'ok' ? (<Button onClick={() => this.showTagModal(record, 'rename')} type="primary">重命名</Button>) : null
                }
              </div>
            </a>
        )
        );
      }
    }, {
      title: '添加runner',
      key: 'addlogRunner',
      dataIndex: 'addlogRunner',
      width: '10%',
      render: (text, record) => {
        return (
            <a>
              <div className="editable-row-operations">
                {this.checkStatus(record.status) === 'ok' ? (<Icon onClick={handleAddRunner} style={{fontSize: 16}} type="plus"/>) : null
                }
              </div>
            </a>
        );
      },

    }, {
      title: '添加Metric Runner',
      key: 'addMetricRunner',
      dataIndex: 'addMetricRunner',
      width: '10%',
      render: (text, record) => {
        return (
            (<a>
              <div className="editable-row-operations">
                {this.checkStatus(record.status) === 'ok' ? (
                  <Icon onClick={handleAddMetricRunner} style={{fontSize: 16}} type="plus"/>) : null
                }
              </div>
            </a>)
        );
      },

    }, {
      title: '停止',
      key: 'stop',
      dataIndex: 'stop',
      width: '3%',
      render: (text, record) => {
        return (<a>
              <div className="editable-row-operations">
                {this.checkStatus(record.status) === 'ok' ? (
                  <Icon onClick={() => this.showTagModal(record, 'stop')} title={"停止Runner"} style={{fontSize: 16}}
                        type='poweroff'/>) : null
                }
              </div>
            </a>

        );
      },
    }, {
      title: '重启',
      key: 'start',
      dataIndex: 'start',
      width: '3%',
      render: (text, record) => {
        return (<a>
              <div className="editable-row-operations">
                {this.checkStatus(record.status) === 'ok' ? (
                  <Icon onClick={() => this.showTagModal(record, 'start')} title={"重启Runner"} style={{fontSize: 16}}
                        type='caret-right'/>) : null
                }
              </div>
            </a>

        );
      },
    }, {
      title: '删除',
      dataIndex: 'delete',
      key: 'delete',
      width: '10%',
      render: (text, record) => {
        return (
            <a>
              <div className="editable-row-operations">
                {this.checkStatus(record.status) !== 'bad' ? (
                  <Icon onClick={() => this.showDeleteTag(record)} style={{fontSize: 16}} type="delete"/>) : null
                }
              </div>
            </a>
        );
      }
    }];
    return (
        <Table columns={columns} pagination={{size: 'small', pageSize: 20}} dataSource={dataSource}/>
    )
  }


  render() {
    return (
        <div>
          {this.renderTagList()}
          <Modal title={this.state.currentModalTitle} visible={this.state.isShowTagModal}
                 onOk={this.handleTagModal} onCancel={this.handleTagModalCancel}
          >
            <FormItem label="名称">
              {this.state.currentModalType === 'rename' ? (
                  <Input onChange={this.changeTagName} placeholder="新tag名称"/>) : (
                  <Input onChange={this.changeRunnerName} placeholder="指定该标签下面具体的runner名称"/>
              ) }</FormItem>
          </Modal>

          <Modal title="是否删除Tag？" visible={this.state.isShowDeleteTag}
                 onOk={this.handleDeleteTag} onCancel={this.handleDeleteTagCancel}
          >
          </Modal>
        </div>
    );
  }
}
export default Form.create()(TagTable);