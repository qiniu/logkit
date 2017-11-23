import React, {Component} from 'react';
import moment from 'moment'
import {
  Table,
  Icon,
  Form
} from 'antd';
import {
  getClusterSlaves,
} from '../../services/logkit';
import * as uuid from 'uuid'
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
    this.init()
  }

  componentWillUnmount() {
  }

  componentDidUpdate(prevProps) {

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
                  <Icon title={"添加runner"} onClick={() => handleAddRunner(record.machineUrl,'machine')} style={{fontSize: 16}} type="plus-circle-o"/>
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
                  <Icon title={"添加metric runner"} onClick={() => handleAddMetricRunner(record.machineUrl,'machine')} style={{fontSize: 16}} type="plus-circle-o"/>
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