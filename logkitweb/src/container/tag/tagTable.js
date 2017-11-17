import React, {Component} from 'react';
import moment from 'moment'
import ClipboardButton from 'react-clipboard.js';
import {Table, Icon, Popconfirm, Button, notification, Modal, Row, Col, Tag, Input, Layout, Menu, Breadcrumb, Form } from 'antd';
import {
  getRunnerConfigs, deleteClusterSlaveTag, postClusterStopSlaveTag, postClusterStartSlaveTag, getClusterRunnerConfigs, postClusterSlaveTag, deleteConfigData, getClusterSlaves, getRunnerStatus, getClusterRunnerStatus, getRunnerVersion, resetConfigData, startRunner, stopRunner
} from '../../services/logkit';
import { titles } from './constant'
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

  }

  componentWillUnmount() {
  }

  componentDidUpdate(prevProps) {

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
        url: this.state.currentTag.machineUrl,
        body: {tag: this.state.currentTagName}
      }).then(data => {
        if (data.success) {
          notification.success({message: "重置成功", duration: 10,})
        }
        this.setState({
          isShowRename: false
        })
      })
    } else if (this.state.currentModalType == 'stop') {
      postClusterStopSlaveTag({
        name: this.state.currentRunnerName,
        tag: this.state.currentTag.name,
        url: this.state.currentTag.machineUrl
      }).then(data => {
        if(!data){
          notification.success({message: '关闭成功', duration: 10})
        }
      })
    } else if (this.state.currentModalType == 'start') {
      postClusterStartSlaveTag({
        name: this.state.currentRunnerName,
        tag: this.state.currentTag.name,
        url: this.state.currentTag.machineUrl
      }).then(data => {
        if(!data){
          notification.success({message: '开启成功', duration: 10})
        }
      })
    }
  }

  handleDeleteTag = () => {
    deleteClusterSlaveTag({name: this.state.currentTag.name, url: this.state.currentTag.machineUrl}).then(data => {
      if (data.success) {
        notification.success({message: "重置成功", duration: 10,})
      }
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
      currentTagName : e.target.value
    })
  }

  changeRunnerName = (e) => {
    console.log(e.target.value)
    this.setState({
      currentRunnerName : e.target.value
    })
  }



  renderTagList() {
    let dataSource = []
    const { tags, handleAddRunner, handleAddMetricRunner } = this.props

    tags.map((item) => {
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
      width: '10%',
    }, {
      title: '机器地址',
      dataIndex: 'machineUrl',
      key: 'machineUrl',
    }, {
      title: '错误信息',
      dataIndex: 'error',
      key: 'error',
    }, {
      title: '重命名',
      dataIndex: 'rename',
      key: 'rename',
      width: '10%',
      render: (text, record) => {
        return (
            (<a>
              <div className="editable-row-operations">
                {
                  <Button onClick={() => this.showTagModal(record,'rename')}  type="primary">重命名</Button>
                }
              </div>
            </a>)
        );
      }
    }, {
      title: '添加runner',
      key: 'addlogRunner',
      dataIndex: 'addlogRunner',
      width: '10%',
      render: (text, record) => {
        return (
            (<a>
              <div className="editable-row-operations">
                {
                  <Icon onClick={handleAddRunner} style={{fontSize: 16}} type="plus"/>
                }
              </div>
            </a>)
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
                {
                  <Icon onClick={handleAddMetricRunner} style={{fontSize: 16}} type="plus"/>
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
        return ((<a>
              <div className="editable-row-operations">
                {
                  <Icon onClick={() => this.showTagModal(record,'stop')} title={"停止Runner"} style={{fontSize: 16}} type='poweroff'/>
                }
              </div>
            </a>)
        );
      },
    }, {
      title: '重启',
      key: 'start',
      dataIndex: 'start',
      width: '3%',
      render: (text, record) => {
        return ((<a>
              <div className="editable-row-operations">
                {
                  <Icon onClick={() => this.showTagModal(record,'start')} title={"重启Runner"} style={{fontSize: 16}} type='caret-right'/>
                }
              </div>
            </a>)
        );
      },
    }, {
      title: '删除',
      dataIndex: 'delete',
      key: 'delete',
      width: '10%',
      render: (text, record) => {
        return (
            (<a>
              <div className="editable-row-operations">
                {
                  <Icon onClick={() => this.showDeleteTag(record)} style={{fontSize: 16}} type="delete"/>
                }
              </div>
            </a>)
        );
      }
    }];
    return (
        <Table columns={columns} pagination={{size: 'small', pageSize: 20}} dataSource={dataSource}/>
    )
  }


  render() {
    const { tags } = this.props
    return (
        <div>
          {this.renderTagList()}
          <Modal title={this.state.currentModalTitle} visible={this.state.isShowTagModal}
                 onOk={this.handleTagModal} onCancel={this.handleTagModalCancel}
          >
            <FormItem label="名称">
              {this.state.currentModalType === 'rename' ? (<Input onChange={this.changeTagName} placeholder="新tag名称" />) : (<Input onChange={this.changeRunnerName} placeholder="指定runner名称" />
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