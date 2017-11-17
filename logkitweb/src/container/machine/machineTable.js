import React, {Component} from 'react';
import moment from 'moment'
import ClipboardButton from 'react-clipboard.js';
import {Table, Icon, Popconfirm, Button, notification, Modal, Row, Col, Tag, Input, Layout, Menu, Breadcrumb, Form } from 'antd';
import {
  getRunnerConfigs, deleteClusterSlaveTag, getClusterRunnerConfigs, postClusterSlaveTag, deleteConfigData, getClusterSlaves, getRunnerStatus, getClusterRunnerStatus, getRunnerVersion, resetConfigData, startRunner, stopRunner
} from '../../services/logkit';
import config from '../../store/config'
import _ from "lodash";


class MachineTable extends Component {
  constructor(props) {
    super(props);
    this.state = {
      current: 0,
      currentItem: '',
      status: [],
      machines: []
    };
  }

  componentDidMount() {

  }

  componentWillUnmount() {
  }

  componentDidUpdate(prevProps) {

  }

  renderMachineList() {
    let dataSource = []
    const { machines, handleAddRunner, handleAddMetricRunner } = this.props
    //console.log(this.state.machines)
    machines.map((item) => {
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
                  <Icon  onClick={handleAddRunner} style={{fontSize: 16}} type="plus"/>
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

    }];
    return (
        <Table columns={columns} pagination={{size: 'small', pageSize: 20}} dataSource={dataSource}/>
    )
  }


  render() {
    return (
        <div>
          {this.renderMachineList()}
        </div>
    );
  }
}
export default Form.create()(MachineTable);