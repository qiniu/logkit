import React, {Component} from 'react';
import {Table, Icon, Popconfirm, Button, notification} from 'antd';
import {
  getRunnerConfigs,deleteConfigData
} from './services/logkit';
import _ from "lodash";

class List extends Component {
  constructor(props) {
    super(props);
    this.state = {
      runners: [],
    };
  }

  componentDidMount() {
    this.init()
  }

  componentWillUnmount() {
  }

  componentDidUpdate(prevProps) {
  }

  init = () => {
    getRunnerConfigs().then(data => {
      if (data.success) {
        this.setState({
          runners: _.omit(data, 'success')
        })
      }
    })
  }

  deleteRunner = (record) => {
    deleteConfigData({name: record.name}).then(data => {
        notification.success({message: "删除成功", duration: 10,})
        this.init()
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
      width: '10%'
    }, {
      title: '发送数量',
      dataIndex: 'sendNumber',
      width: '10%'
    }, {
      title: '解析成功条数',
      dataIndex: 'parseSuccessNumber',
      width: '10%'
    }, {
      title: '解析失败条数',
      dataIndex: 'parseFailNUmber',
      width: '10%'
    }, {
      title: '发送成功条数',
      dataIndex: 'successNumber',
      width: '10%'
    }, {
      title: '发送失败条数',
      dataIndex: 'failNUmber',
      width: '10%'
    }, {
      title: '创建时间',
      dataIndex: 'time',
      width: '10%'
    }, {
      title: '路径',
      dataIndex: 'path',
      width: '10%'
    }, {
      title: 'Edit',
      key: 'edit',
      dataIndex: 'edit',
      width: '10%',
      render: (text, record) => {
        return (
            <a>
              <div className="editable-row-operations">
                {
                  <Popconfirm title="是否删除该Runner?" onConfirm={() => this.deleteRunner(record)}>
                    <Icon style={{fontSize: '15px'}} type="delete"/>
                  </Popconfirm>
                }
              </div>
            </a>
        );
      },

    }];

    let data = []
    if (this.state.runners != null) {
      _.values(this.state.runners).map((item, value) => {
        console.log()
        data.push({
          key: item.name,
          name: item.name,
          status: item.name,
          sendNumber: item.name,
          parseSuccessNumber: item.parserStats.success,
          parseFailNUmber: item.parserStats.error,
          successNumber: item.parserStats.success,
          failNUmber: item.parserStats.error,
          time: item.name,
          path: item.logpath
        })
      })

    }

    return (
        <Table columns={columns} pagination={{size: 'small', pageSize: 10}} dataSource={data}/>
    )
  }

  add = () => {
    this.props.router.push({pathname: `/index/create`})
  }

  render() {
    return (
        <div className="logkit-container">
          <div className="header">七牛Logkit配置文件助手</div>
          <div className="content">
            <Button type="dashed" onClick={this.add}>
              <Icon type="plus"/> 增加
            </Button>
            {this.renderRunnerList()}
          </div>
        </div>
    );
  }
}
export default List;