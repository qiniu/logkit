import React, {Component} from 'react';
import {
  Table,
  Icon,
  notification,
  Modal,
  Input,
  Form,
  Select
} from 'antd';
import {
  deleteClusterSlaveTag,
  postClusterStopSlaveTag,
  postClusterStartSlaveTag,
  postClusterSlaveTag,
  getClusterSlaves,
  getRunnersByTagOrMachineUrl
} from '../../services/logkit';
import {titles} from './constant'
import _ from "lodash";
import * as uuid from 'uuid'
const FormItem = Form.Item;
const Option = Select.Option

class TagTable extends Component {
  constructor(props) {
    super(props);
    this.state = {
      status: [],
      machines: [],
      runners: [],
      tags: [],
      isShowTagModal: false,
      isShowDeleteTag: false,
      currentTag: '',
      currentTagName: '',
      currentModalType: '',
      currentRunnerName: '',
      isLoading: false
    };

  }

  componentDidMount() {
    this.init()
  }

  componentWillUnmount() {
  }

  componentDidUpdate(prevProps) {

  }

  getClusterSLave = () => {
    getClusterSlaves().then(data => {
      if (data.code === 'L200') {
        this.setState({
          tags: _.values(data.data),
          isLoading: false
        })
      }
    })
  }

  init() {
    let that = this
    this.getClusterSLave()
    window.tagInterval = setInterval(function () {
      that.getClusterSLave()
    }, 15000)
  }

  getRunnersByTag = (item) => {
    getRunnersByTagOrMachineUrl({ tag: item.name, url: ''  }).then(item => {
      if (item.code === 'L200') {
        this.setState({
          runners: item.data
        })
      }
    })
  }

  showTagModal = (item, type) => {
    if (type !== 'rename') {
      this.getRunnersByTag(item)
    }
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
    this.setState({
      isLoading: true
    })
    if (this.state.currentModalType == 'rename') {
      postClusterSlaveTag({
        name: this.state.currentTag.name,
        url: '',
        body: {tag: this.state.currentTagName}
      }).then(item => {
        if (item.code === 'L200') {
          notification.success({message: "重置成功", duration: 10,})
          this.setState({
            isShowTagModal: false
          })
          this.getClusterSLave()
        }

      })
    } else if (this.state.currentModalType == 'stop') {
      postClusterStopSlaveTag({
        name: this.state.currentRunnerName,
        tag: this.state.currentTag.name,
        url: ''
      }).then(item => {
        if (item.code === 'L200') {
          notification.success({message: '关闭成功', duration: 10})
          this.getClusterSLave()
        }
      })
    } else if (this.state.currentModalType == 'start') {
      postClusterStartSlaveTag({
        name: this.state.currentRunnerName,
        tag: this.state.currentTag.name,
        url: ''
      }).then(item => {
        if (item.code === 'L200') {
          notification.success({message: '开启成功', duration: 10})
          this.getClusterSLave()
        }
      })
    }
  }

  handleDeleteTag = () => {
    this.setState({
      isLoading: true
    })
    deleteClusterSlaveTag({name: this.state.currentTag.name, url: ''}).then(item => {
      if (item.code === 'L200') {
        notification.success({message: "重置成功", duration: 10,})
        this.setState({
          isShowDeleteTag: false
        })
        this.getClusterSLave()
      }
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
    this.setState({
      currentTagName: e.target.value
    })
  }

  changeRunnerName = (value) => {
    this.setState({
      currentRunnerName: value
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
    if (_.includes(status, 'lost')) {
      return 'lost'
    }
    else if (_.includes(status, 'bad')) {
      return 'bad'
    }
    else {
      return 'ok'
    }
  }


  renderTagList() {
    let dataSource = []
    const {handleAddRunner, handleAddMetricRunner} = this.props
    let tags = []
    let machineUrl = []
    let status = []

    _.sortBy(this.state.tags, 'tag').map((item, i) => {
      if (_.includes(tags,item.tag)) {
        dataSource[_.findIndex(dataSource, 'name', item.tag)].machineUrl.push(item.url)
        dataSource[_.findIndex(dataSource, 'name', item.tag)].status.push(item.status)
      }else {
        tags.push(item.tag)
        dataSource.push({
          key: uuid(),
          name:item.tag,
          machineUrl:[item.url],
          status:[item.status]
        })
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
                    { this.checkStatus(record.status) === 'ok' ? (
                    <Icon style={{fontSize: 16}} type="setting" title="tag重命名" onClick={() => this.showTagModal(record, 'rename')} />) : null
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
                {this.checkStatus(record.status) === 'ok' ? (
                    <Icon title={"添加runner"}  onClick={() => handleAddRunner(record.name,'tag')} style={{fontSize: 16}} type="plus-circle-o"/>) : null
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
                    <Icon title={"添加metric runner"} onClick={() => handleAddMetricRunner(record.name,'tag')} style={{fontSize: 16}} type="plus-circle-o"/>) : null
                }
              </div>
            </a>)
        );
      },
    }, {
      title: '停止',
      key: 'stop',
      dataIndex: 'stop',
      width: '6%',
      render: (text, record) => {
        return (<a>
              <div className="editable-row-operations">
                {this.checkStatus(record.status) === 'ok' ? (
                    <Icon onClick={() => this.showTagModal(record, 'stop')} title={"停止该tag对应的runner"} style={{fontSize: 16}}
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
      width: '6%',
      render: (text, record) => {
        return (<a>
              <div className="editable-row-operations">
                {this.checkStatus(record.status) === 'ok' ? (
                    <Icon onClick={() => this.showTagModal(record, 'start')} title={"重启该tag对应的runner"} style={{fontSize: 16}}
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
      width: '6%',
      render: (text, record) => {
        return (
            <a>
              <div className="editable-row-operations">
                {this.checkStatus(record.status) !== 'bad' ? (
                    <Icon onClick={() => this.showDeleteTag(record)} title={"删除该tag对应的runner"} style={{fontSize: 16}} type="delete"/>) : null
                }
              </div>
            </a>
        );
      }
    }];
    return (
        <Table columns={columns} pagination={{size: 'small', pageSize: 20}} dataSource={dataSource} loading={this.state.isLoading}  />
    )
  }

  renderSelectOptions = (items) => {
    let options = []
    if (items != undefined) {
      items.map((ele) => {
        options.push(<Option key={ele} value={ele}>{ele}</Option>)
      })
    }
    return (
        options
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
                  <Input key="rename" onChange={this.changeTagName} placeholder="新tag名称"/>) : (
              <Select style={{width: '200px'}} key="opt" onChange={this.changeRunnerName} placeholder="选择该标签下的一个runner" >
                {this.renderSelectOptions(this.state.runners)}
              </Select>
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