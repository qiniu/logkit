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
const {Header, Content, Footer, Sider} = Layout;

class List extends Component {
  constructor(props) {
    super(props);
    this.state = {
      runners: [],
      status: [],
      currentItem: '',
      version: '',
      collapsed: true,
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

  init = () => {
    getRunnerVersion().then(item => {
      if (item.code === 'L200') {
        window.version = item.data
      }
    })

    getIsCluster().then(item => {
      if (item.data === true) {
        window.isCluster = true
      } else {
        window.isCluster = false
      }
    })

  }

  turnToConfigPage() {
    this.props.router.push({pathname: `/index/create?copyConfig=true`})
  }

  addLogRunner = () => {
    this.setState({
      currentMenu: 'createLog'
    })
    window.isCopy = false
    window.nodeCopy = null
  }

  turnToRunnerTab = () => {
    this.setState({
      currentMenu: 'runner'
    })
  }

  addMetricRunner = () => {
    this.setState({
      currentMenu: 'createMetricLog'
    })
    window.isCopy = false
    window.nodeCopy = null
  }

  onCollapse = (collapsed) => {
    this.setState({collapsed});
  }

  TurnToLogRunner = () => {
    this.setState({
      currentMenu: 'createLog'
    })
    window.isCopy = true
  }

  TurnToMetricRunner = () => {
    this.setState({
      currentMenu: 'createMetricLog'
    })
    window.isCopy = true
  }


  changeMenu = (e) => {
    this.setState({
      currentMenu: e.key
    })
    if (e.key !== 'runner') {
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
                <img style={{marginLeft: '15px'}} src='../../../static/logkit100.png'></img>) : (
                <img src='../../../static/favicon.ico'></img>)}</div>
            <Menu theme="dark" defaultSelectedKeys={['runner']} mode="inline" onClick={this.changeMenu}>
              {window.isCluster === true ? (<Menu.Item key="tag">
                    <Icon type="tags-o"/>
                    <span>标签</span>
                  </Menu.Item>
              ) : null}
              {window.isCluster === true ? (<Menu.Item key="machine">
                <Icon type="desktop"/>
                <span>机器</span>
              </Menu.Item>) : null}
              <Menu.Item key="runner">
                <Icon type="file"/>
                <span>Runner管理</span>
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
                  <MachineTable handleAddRunner={this.addLogRunner.bind(this)}
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
                  <RunnerTable handleAddRunner={this.addLogRunner.bind(this)}
                               handleAddMetricRunner={this.addMetricRunner.bind(this)}
                               handleTurnToRunner={this.TurnToLogRunner.bind(this)}
                               handleTurnToMetricRunner={this.TurnToMetricRunner.bind(this)}
                               runners={this.state.runners} runnerStatus={this.state.status}
                  />
                </div>

              </div>
            </Content>) : null}
            {this.state.currentMenu === 'createLog' ? (<Content style={{margin: '0 16px'}}>
              <Breadcrumb style={{margin: '16px 0'}}>
                <Breadcrumb.Item>创建runner</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{padding: 24, background: '#fff', minHeight: 360}}>
                <div className="content">
                  <CreateLogRunner handleTurnToRunner={this.turnToRunnerTab.bind(this)}/>
                </div>

              </div>
            </Content>) : null}
            {this.state.currentMenu === 'createMetricLog' ? (<Content style={{margin: '0 16px'}}>
              <Breadcrumb style={{margin: '16px 0'}}>
                <Breadcrumb.Item>创建metric runner</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{padding: 24, background: '#fff', minHeight: 360}}>
                <div className="content">
                  <CreateMetricRunner handleTurnToRunner={this.turnToRunnerTab.bind(this)}/>
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