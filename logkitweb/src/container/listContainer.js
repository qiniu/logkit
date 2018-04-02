import React, {Component} from 'react';
import {
  getRunnerVersion,
  getIsCluster
} from '../services/logkit';
import CreateLogRunner from './createLogContainer'
import RunnerTable from './runner/runnerTable'
import MachineTable from './machine/machineTable'
import CreateMetricRunner from './createMetricContainer'
import TagTable from './tag/tagTable'
import {
  Icon,
  Tag,
  Layout,
  Menu,
  Breadcrumb,
} from 'antd';
import config from '../store/config'
const {Header, Content, Footer, Sider} = Layout;

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
      machines: [],
      currentTagName: '',
      currentMachineUrl: '',
      isCluster: false
    };
    this.init()
  }

  componentDidMount() {

  }

  componentWillUnmount() {

  }

  componentDidUpdate(prevProps) {

  }

  init = () => {
    getRunnerVersion().then(item => {
      if (item.code === 'L200') {
        this.setState({
          version: item.data.version
        })
      }
    })
    getIsCluster().then(item => {
      if (item.code === 'L200') {
        window.isCluster = item.data
        this.setState({ isCluster: item.data});
      }
    })

  }

  initInterval = () => {
    window.clearInterval(window.tagInterval);
    window.clearInterval(window.machineInterval);
    window.clearInterval(window.statusInterval);
  }

  turnToConfigPage() {
    this.props.router.push({pathname: `/index/create?copyConfig=true`})
  }

  addLogRunner = (record, type) => {
    config.setNodeData({})
    if (type === 'tag') {
      this.setState({
        currentMenu: 'createLog',
        currentTagName: record,
        currentMachineUrl: ''
      })
    } else {
      this.setState({
        currentMenu: 'createLog',
        currentMachineUrl: record,
        currentTagName: ''
      })
    }

    window.isCopy = false
    window.nodeCopy = null
    this.initInterval()
  }

  turnToRunnerTab = () => {
    this.setState({
      currentMenu: 'runner'
    })
  }

  addMetricRunner = (record,type) => {
    config.setNodeData({})
    if (type === 'tag') {
      this.setState({
        currentMenu: 'createMetricLog',
        currentTagName: record,
        currentMachineUrl: ''
      })
    } else {
      this.setState({
        currentMenu: 'createMetricLog',
        currentMachineUrl: record,
        currentTagName: ''
      })
    }
    window.isCopy = false
    window.nodeCopy = null
    this.initInterval()
  }

  onCollapse = (collapsed) => {
    this.setState({collapsed});
  }

  TurnToLogRunner = () => {
    this.setState({
      currentMenu: 'createLog'
    })
    window.isCopy = true
    this.initInterval()
  }

  TurnToMetricRunner = () => {
    this.setState({
      currentMenu: 'createMetricLog'
    })
    window.isCopy = true
    this.initInterval()
  }


  changeMenu = (e) => {
    this.setState({
      currentMenu: e.key
    })
    if (e.key === 'runner') {
      window.clearInterval(window.tagInterval);
      window.clearInterval(window.machineInterval);
    } else if (e.key === 'tag') {
      window.clearInterval(window.statusInterval);
      window.clearInterval(window.machineInterval);
    } else if (e.key === 'machine') {
      window.clearInterval(window.tagInterval);
      window.clearInterval(window.statusInterval);
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
            <img style={{ marginLeft: '15px' }} src='../../../static/logkitIcon.png'></img>) : (
                <img src='../../../static/favicon.ico'></img>)}</div>
            <Menu theme="dark" defaultSelectedKeys={['runner']} mode="inline" onClick={this.changeMenu}>
              {window.isCluster === true ? (<Menu.Item key="tag">
                    <Icon type="tags-o"/>
                    <span>集群管理</span>
                  </Menu.Item>
              ) : null}
              {window.isCluster === true ? (<Menu.Item key="machine">
                <Icon type="desktop"/>
                <span>机器管理</span>
              </Menu.Item>) : null}
              <Menu.Item key="runner">
                <Icon type="file"/>
                <span>收集器(runner)管理</span>
              </Menu.Item>
            </Menu>
          </Sider>
          <Layout>
            <Header className="header" style={{background: '#fff', padding: 0}}> 
              七牛Logkit监控中心{this.state.version}
              <div style={{float: 'right'}}>
                <a target="_blank" href="https://github.com/qiniu/logkit">
                <Tag color="#108ee9"><Icon type="github" style={{ fontSize: 12, color: 'white' }} />logkit</Tag> </a>
                <a target="_blank" href="https://github.com/qiniu/logkit/wiki">
                <Tag color="#108ee9"><Icon type="question-circle-o" style={{ fontSize: 12, color: 'white' }} />帮助文档</Tag> </a>
                <a target="_blank" href="https://qiniu.github.io/pandora-docs/#/"><Tag
                  color="#108ee9">Pandora产品</Tag>
                </a>
              </div>
            </Header>
            {this.state.currentMenu === 'tag' ? (<Content style={{margin: '0 16px'}}>
              <Breadcrumb style={{margin: '16px 0'}}>
                <Breadcrumb.Item>集群管理列表</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{padding: 24, background: '#fff', minHeight: 800}}>
                <div className="content">
                  <TagTable handleAddRunner={this.addLogRunner.bind(this)}
                            handleAddMetricRunner={this.addMetricRunner.bind(this)}
                            handleTurnToRunner={this.TurnToLogRunner.bind(this)}
                            handleTurnToMetricRunner={this.TurnToMetricRunner.bind(this)}/>
                </div>

              </div>
            </Content>) : null}
            {this.state.currentMenu === 'machine' ? (<Content style={{margin: '0 16px'}}>
              <Breadcrumb style={{margin: '16px 0'}}>
                <Breadcrumb.Item>机器管理列表</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{padding: 24, background: '#fff', minHeight: 800}}>
                <div className="content">
                  <MachineTable handleAddRunner={this.addLogRunner.bind(this)}
                                handleAddMetricRunner={this.addMetricRunner.bind(this)}/>
                </div>
              </div>
            </Content>) : null}
            {this.state.currentMenu === 'runner' ? (<Content style={{margin: '0 16px'}}>
              <Breadcrumb style={{margin: '16px 0'}}>
                <Breadcrumb.Item>收集器(runner)管理列表</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{padding: 24, background: '#fff', minHeight: 800}}>
                <div className="content">
                  <RunnerTable isCluster={this.state.isCluster} handleAddRunner={this.addLogRunner.bind(this)}
                               handleAddMetricRunner={this.addMetricRunner.bind(this)}
                               handleTurnToRunner={this.TurnToLogRunner.bind(this)}
                               handleTurnToMetricRunner={this.TurnToMetricRunner.bind(this)}
                  />
                </div>

              </div>
            </Content>) : null}
            {this.state.currentMenu === 'createLog' ? (<Content style={{margin: '0 16px'}}>
              <Breadcrumb style={{margin: '16px 0'}}>
                <Breadcrumb.Item>{window.isCopy ? '修改日志收集器' : '创建日志收集器'}</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{padding: 24, background: '#fff', minHeight: 800}}>
                <div className="content">
                  <CreateLogRunner currentTagName={this.state.currentTagName} currentMachineUrl={this.state.currentMachineUrl} handleTurnToRunner={this.turnToRunnerTab.bind(this)}/>
                </div>

              </div>
            </Content>) : null}
            {this.state.currentMenu === 'createMetricLog' ? (<Content style={{margin: '0 16px'}}>
              <Breadcrumb style={{margin: '16px 0'}}>
                <Breadcrumb.Item>创建系统信息收集器</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{padding: 24, background: '#fff', minHeight: 800}}>
                <div className="content">
                  <CreateMetricRunner currentTagName={this.state.currentTagName} currentMachineUrl={this.state.currentMachineUrl} handleTurnToRunner={this.turnToRunnerTab.bind(this)}/>
                </div>

              </div>
            </Content>) : null}
            <Footer style={{textAlign: 'center'}}>
              七牛logkit监控中心 <Icon type="copyright" />2018 七牛云
            </Footer>
          </Layout>


        </Layout>

    );
  }
}
export default List;