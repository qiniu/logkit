import React, {Component} from 'react';
import ClipboardButton from 'react-clipboard.js';
import {
  getRunnerConfigs,
  deleteClusterSlaveTag,
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
} from '../services/logkit';
import CreateLogRunner from './createLogContainer'
import RunnerTable from './runner/runnerTable'
import MachineTable from './machine/machineTable'
import CreateMetricRunner from './createMetricContainer'
import TagTable from './tag/tagTable'
import _ from "lodash";
import {
  Icon,
  Tag,
  Layout,
  Menu,
  Breadcrumb,
  Form
} from 'antd';
const SubMenu = Menu.SubMenu;
const {Header, Content, Footer, Sider} = Layout;
const FormItem = Form.Item;


class List extends Component {
  constructor(props) {
    super(props);
    this.state = {
      runners: [],
      status: [],
      currentItem: '',
      version: '',
      collapsed: false,
      currentMenu: 'runner',
      machines: []
    };
    this.init()
  }

  componentDidMount() {

  }

  componentWillUnmount() {

  }

  componentDidUpdate(prevProps) {

  }

  onCollapse = (collapsed) => {
    this.setState({collapsed, logSrc: '../../../static/favicon.ico'});
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


  getStatus = () => {
    let that = this
    getClusterRunnerConfigs({tag: window.tag ?  window.tag : '', machineUrl:  window.machine_url ? window.machine_url : ''}).then(data => {
      if (data.success) {
        let mapData = _.omit(data, 'success')
        let tagMapData = this.transformRunner(mapData)
        that.setState({
          runners: tagMapData
        })
        getClusterRunnerStatus({tag: window.tag ?  window.tag : '', machineUrl:  window.machine_url ? window.machine_url : ''}).then(data => {
          if (data.success) {
            that.setState({
              status: this.transformStatus(_.omit(data, 'success'))
            })
          }
        })
      }
    })

    // getRunnerConfigs().then(data => {
    //   if (data.success) {
    //     //console.log(_.omit(data, 'success'))
    //     let mapData = _.omit(data, 'success')
    //     //let tagMapData = this.transform(mapData)
    //     that.setState({
    //       runners: _.omit(data, 'success')
    //     })
    //     getRunnerStatus().then(data => {
    //       if (data.success) {
    //         that.setState({
    //           status: _.values(_.omit(data, 'success'))
    //         })
    //       }
    //     })
    //   }
    // })

    getClusterSlaves().then(data => {
      if (data.success) {
        that.setState({
          machines: _.values(_.omit(data, 'success'))
        })
      }
    })
  }

  init = () => {
    let that = this
    getRunnerVersion().then(data => {
      if (data.success) {
        that.setState({
          version: _.values(_.omit(data, 'success'))
        })
      }
    })

    that.getStatus()

    if (window.statusInterval !== undefined && window.statusInterval !== 'undefined') {
      window.clearInterval(window.statusInterval);
    }

    window.statusInterval = setInterval(function () {
      that.getStatus()
    }, 8000)

  }

  turnToConfigPage() {
    this.props.router.push({pathname: `/index/create?copyConfig=true`})
  }

  addLogRunner = () => {
    this.setState({
      currentMenu: 'createLog'
    })
    //this.props.router.push({pathname: `/index/create-log-runner`})
  }

  addMetricRunner = () => {
    console.log('test1')
    this.setState({
      currentMenu: 'createMetricLog'
    })
  }


  changeMenu = (e) => {
    this.setState({
      currentMenu: e.key
    })
    if (e.key !== 'runner') {
      window.clearInterval(window.statusInterval);
    } else {
      this.init()
    }
  }

  render() {
    return (
        <Layout style={{minHeight: '100vh'}}>
          <Sider
              collapsible
              collapsed={this.state.collapsed}
              onCollapse={this.onCollapse}
          >
            <div className="logo">{this.state.collapsed === false ? (
                <img style={{marginLeft: '15px'}} src='../../../static/logkit100.png'></img>) : (
                <img src='../../../static/favicon.ico'></img>)}</div>
            <Menu theme="dark" defaultSelectedKeys={['runner']} mode="inline" onClick={this.changeMenu}>
              <Menu.Item key="tag">
                <Icon type="tags-o" />
                <span>标签</span>
              </Menu.Item>
              <Menu.Item key="machine">
                <Icon type="desktop"/>
                <span>机器</span>
              </Menu.Item>
              <Menu.Item key="runner">
                <Icon type="file"/>
                <span>Runner</span>
              </Menu.Item>
            </Menu>
          </Sider>
          <Layout>
            <Header style={{background: '#fff', padding: 0}}/>
            {this.state.currentMenu === 'tag' ? (<Content style={{margin: '0 16px'}}>
              <Breadcrumb style={{margin: '16px 0'}}>
                <Breadcrumb.Item>标签管理列表</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{padding: 24, background: '#fff', minHeight: 360}}>
                <div className="content">
                  <TagTable tags={this.state.runners} handleAddRunner={this.addLogRunner.bind(this)}
                            handleAddMetricRunner={this.addMetricRunner.bind(this)}/>
                </div>

              </div>
            </Content>) : null}
            {this.state.currentMenu === 'machine' ? (<Content style={{margin: '0 16px'}}>
              <Breadcrumb style={{margin: '16px 0'}}>
                <Breadcrumb.Item>机器管理列表</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{padding: 24, background: '#fff', minHeight: 360}}>
                <div className="content">
                  <MachineTable machines={this.state.machines} handleAddRunner={this.addLogRunner.bind(this)}
                                handleAddMetricRunner={this.addMetricRunner.bind(this)}/>
                </div>
              </div>
            </Content>) : null}
            {this.state.currentMenu === 'runner' ? (<Content style={{margin: '0 16px'}}>
              <Breadcrumb style={{margin: '16px 0'}}>
                <Breadcrumb.Item>Runner管理列表</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{padding: 24, background: '#fff', minHeight: 360}}>
                <div className="content">
                  <RunnerTable runners={this.state.runners} runnerStatus={this.state.status}
                               searchRunner={this.getStatus.bind(this)}/>
                </div>

              </div>
            </Content>) : null}
            {this.state.currentMenu === 'createLog' ? (<Content style={{margin: '0 16px'}}>
              <Breadcrumb style={{margin: '16px 0'}}>
                <Breadcrumb.Item>创建runner</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{padding: 24, background: '#fff', minHeight: 360}}>
                <div className="content">
                  <CreateLogRunner/>
                </div>

              </div>
            </Content>) : null}
            {this.state.currentMenu === 'createMetricLog' ? (<Content style={{margin: '0 16px'}}>
              <Breadcrumb style={{margin: '16px 0'}}>
                <Breadcrumb.Item>创建metric runner</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{padding: 24, background: '#fff', minHeight: 360}}>
                <div className="content">
                  <CreateMetricRunner/>
                </div>

              </div>
            </Content>) : null}
            <Footer style={{textAlign: 'center'}}>
              更多信息请访问：
              <a target="_blank" href="https://github.com/qiniu/logkit">
                <Tag color="#108ee9">Logkit</Tag> </a> |
              <a target="_blank" href="https://github.com/qiniu/logkit/wiki">
                <Tag color="#108ee9">帮助文档</Tag> </a> |
              <a target="_blank" href="https://qiniu.github.io/pandora-docs/#/"><Tag
                  color="#108ee9">Pandora产品</Tag>
              </a>
            </Footer>
          </Layout>


        </Layout>

    );
  }
}
export default List;