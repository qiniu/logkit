import React, {Component} from 'react';
import moment from 'moment'
import {
  Table,
  Icon,
  notification,
  Modal,
  Input,
  Form,
  Select,
  Popconfirm,
} from 'antd';
import {
  getClusterSlaves,
  postClusterSlaveTag,
  postClusterDeleteSlaveTag,
  deleteClusterSlaveTag
} from '../../services/logkit';
import * as uuid from 'uuid'
import {titles} from '../tag/constant'
import config from '../../store/config'
import _ from "lodash";
const FormItem = Form.Item;
const Option = Select.Option


class MachineTable extends Component {
  constructor(props) {
    super(props);
    this.state = {
      current: 0,
      currentItem: '',
      status: [],
      machines: [],
      isShowTagModal: false,
      currentTag: '',
      currentTagName: '',
      currentModalType: '',
    };
  }

  componentDidMount() {
    this.init()
  }

  componentWillUnmount() {
  }

  componentDidUpdate(prevProps) {

  }

  showTagModal = (item, type) => {
    this.setState({
      currentTag: item,
      isShowTagModal: true,
      currentModalType: type,
      currentModalTitle: titles[type]
    })
  }

  deleteSlave = (param) => {
    deleteClusterSlaveTag({
      name: param.name,
      url: param.machineUrl
    }).then(item => {
      if(item.code === 'L200') {
        notification.success({message: "删除成功", duration: 10,})
        this.getClusterSLave()
      }
    })
  }

  handleTagModal = () => {
    this.setState({
      isLoading: true
    })
    if (this.state.currentModalType == 'rename') {
      postClusterSlaveTag({
        name: this.state.currentTag.name,
        url: this.state.currentTag.machineUrl,
        body: {tag: this.state.currentTagName}
      }).then(item => {
        if (item.code === 'L200') {
          notification.success({message: "重命名成功", duration: 10,})
          this.setState({
            isShowTagModal: false
          })
          this.getClusterSLave()
        }

      })
    } else if (this.state.currentModalType == 'delete') {
      deleteClusterSlaveTag({
        tag: this.state.currentTag.name,
        url: this.state.currentTag.machineUrl
      }).then(item => {
        if (item.code === 'L200') {
          notification.success({message: '删除成功', duration: 10})
          this.setState({
            isShowTagModal: false
          })
          this.getClusterSLave()
        }
      })
    }
  }

  handleTagModalCancel = () => {
    this.setState({
      isShowTagModal: false
    })
  }

  changeTagName = (e) => {
    this.setState({
      currentTagName: e.target.value
    })
  }

  getClusterSLave = () => {
    getClusterSlaves().then(item => {
      if (item.code === 'L200') {
        this.setState({
          machines: _.values(item.data)
        })
      }
    })
  }

  init() {
    let that = this
    this.getClusterSLave()
    window.machineInterval = setInterval(function () {
      that.getClusterSLave()
    }, 15000)
  }

  renderMachineList() {
    let dataSource = []
    const {machines, handleAddRunner, handleAddMetricRunner} = this.props
    this.state.machines.map((item) => {
      dataSource.push({
        key: uuid(),
        name: item.tag,
        machineUrl: item.url,
        status: item.status,
        last_touch: moment(item.last_touch).format("YYYY-MM-DD HH:mm:ss")
      })
    })

    const columns = [{
      title: '机器地址',
      dataIndex: 'machineUrl',
      key: 'machineUrl',
    }, {
      title: '集群名称',
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
      title: '更改集群名称',
      dataIndex: 'rename',
      key: 'rename',
      width: '10%',
      render: (text, record) => {
        return (
            (
                <a>
                  <div className="editable-row-operations">
                    { record.status === 'ok' ? (
                        <Icon style={{fontSize: 16}} type="setting" title="集群重命名" onClick={() => this.showTagModal(record, 'rename')} />) : null
                    }
                  </div>
                </a>
            )
        );
      }
    }, {
      title: '添加收集器',
      key: 'edit',
      dataIndex: 'edit',
      width: '10%',
      render: (text, record) => {
        return (
            (<a>
              <div className="editable-row-operations">
                {
                  <Icon title={"添加收集器"} onClick={() => handleAddRunner(record.machineUrl,'machine')} style={{fontSize: 16}} type="plus-circle-o"/>
                }
              </div>
            </a>)
        );
      },

    }, {
      title: '添加系统信息收集器',
      key: 'addMetricRunner',
      dataIndex: 'addMetricRunner',
      width: '10%',
      render: (text, record) => {
        return (
            (<a>
              <div className="editable-row-operations">
                {
                  <Icon title={"添加系统信息收集器"} onClick={() => handleAddMetricRunner(record.machineUrl,'machine')} style={{fontSize: 16}} type="plus-circle-o"/>
                }
              </div>
            </a>)
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
                {
                  <Popconfirm title="是否删除该机器?" onConfirm={() => this.deleteSlave(record)}>
                    <Icon title={"删除收集器"} style={{fontSize: 16}} type="delete"/>
                  </Popconfirm>
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
          {this.renderMachineList()}
          <Modal title={this.state.currentModalTitle} visible={this.state.isShowTagModal}
                 onOk={this.handleTagModal} onCancel={this.handleTagModalCancel}
          >
            <FormItem label="名称">
              <Input key="rename" onChange={this.changeTagName} placeholder="新集群名称"/></FormItem>
          </Modal>
        </div>

    );
  }
}
export default Form.create()(MachineTable);