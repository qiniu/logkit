import React, {Component} from 'react';
import {notification, Button, Steps, Icon, Tag, Layout} from 'antd';
import Keys from '../components/metricKeys'
import Opt from '../components/metricConfig'
import Usages from '../components/metricUsages'
import Sender from '../components/senderConfig'
import RenderConfig from '../components/renderConfig'
import config from '../store/config'
import {isJSON} from '../utils/tools'
import moment from 'moment'
import {postConfigData, getRunnerVersion, putConfigData} from '../services/logkit';
import _ from "lodash";

const Step = Steps.Step;
const {Header, Content, Footer, Sider} = Layout;
const steps = [{
  title: '系统信息选择',
  content: '配置需要采集的系统信息类型',
}, {
  title: '配置收集字段',
  content: '配置需要收集的字段',
}, {
  title: '配置metric',
  content: '配置相关字段解析',
}, {
  title: '配置发送方式',
  content: '配置相关发送方式',
}, {
  title: '确认并添加Runner',
  content: '确认并添加',
}];
class CreateMetricRunner extends Component {
  constructor(props) {
    super(props);
    this.state = {
      current: 0,
      version: '',
      metricKeys: {},
      metricConfigs: {},
      isCopyStatus: false,
    };
    window.clearInterval(window.statusInterval);
  }

  componentDidMount() {
    this.init()
  }

  componentWillUnmount() {

  }

  componentDidUpdate(prevProps) {

  }

  init = () => {
    let that = this
    let isCopy = this.props.location.query.copyConfig
    if (isCopy === 'true') {
      window.isCopy = true
      this.setState({
        isCopyStatus: true
      })
    } else {
      window.isCopy = false
    }

    getRunnerVersion().then(data => {
      if (data.success) {
        that.setState({
          version: _.values(_.omit(data, 'success'))
        })
      }
    })
  }

  next() {
    let that = this;
    if (this.state.current === 0) {
      if(config.get("metric").length > 0){
        const current = this.state.current + 1;
        this.setState({current});
      }else{
        notification.warning({message: "请至少采集一种系统信息", duration: 20,})
      }
    } else if (this.state.current === 1) {
      let flag = [];
      config.get("metric").map(m => {
        let isHasTrue = false;
        for(let k in this.state.metricKeys[m.type]){
          if(this.state.metricKeys[m.type][k]){
            isHasTrue = true;
            return true;
          }
        }
        if(!isHasTrue && Object.keys(this.state.metricKeys[m.type]).length) flag.push(m.type);
      });
      if(flag.length <= 0){
        const current = this.state.current + 1;
        this.setState({current});
      } else {
        notification.warning({message: "请至少为"+flag.join(", ")+"选择一个采集的字段", duration: 20,})
      }
    } else if (this.state.current === 2) {
      const current = this.state.current + 1;
      this.setState({current});
    } else if (this.state.current === 3) {
      that.refs.checkSenderData.validateFields(null, {}, (err) => {
        if (err) {
          notification.warning({message: "表单校验未通过,请检查", duration: 20,})
        } else {
          const current = this.state.current + 1;
          this.setState({current});
          this.setConfig();
          let name = "runner." + moment().format("YYYYMMDDHHmmss");
          let batch_interval = that.refs.initConfig.getFieldValue('batch_interval')
          let collect_interval = that.refs.initConfig.getFieldValue('collect_interval')
          let runnerName = that.refs.initConfig.getFieldValue('name')
          if (window.isCopy && window.nodeCopy) {
            name = window.nodeCopy.name
          }
          let data = {
            name: runnerName != undefined ? runnerName : name,
            batch_interval: batch_interval != undefined ? batch_interval : 60,
            collect_interval: collect_interval != undefined ? collect_interval : 3,
            ...config.getNodeData()
          }
          that.refs.initConfig.setFieldsValue({config: JSON.stringify(data, null, 2)});
          if (runnerName == undefined) {
            that.refs.initConfig.setFieldsValue({name: name});
          }

          if (batch_interval == undefined) {
            that.refs.initConfig.setFieldsValue({batch_interval: 60});
          }
          if (collect_interval == undefined) {
            that.refs.initConfig.setFieldsValue({collect_interval: 3});
          }
        }
      });
    }
  }

  handleMetricKeys = (metricKeys) => {
    this.state.metricKeys = metricKeys;
  }

  handleMetricConfigs = (metricConfigs) => {
    this.state.metricConfigs = metricConfigs;
  }

  setConfig = () => {
    let configData = [];
    let metric = config.get("metric");
    if(!metric) return;
    metric.map((m, _) => {
      m["attributes"] = this.state.metricKeys[m.type];
      m["config"] = this.state.metricConfigs[m.type];
      configData.push(m);
    });
    config.set("metric", configData);
  }

  addRunner = () => {
    let that = this
    const {validateFields, getFieldsValue} =  that.refs.initConfig;
    let formData = getFieldsValue();
    validateFields(null, {}, (err) => {
      if (err) {
        notification.warning({message: "表单校验未通过,请检查", duration: 20,})
        return
      } else {
        if (isJSON(formData.config)) {
          let data = JSON.parse(formData.config);
          postConfigData({name: data.name, body: data}).then(data => {
            if (data === undefined) {
              notification.success({message: "Runner添加成功", duration: 10,})
              this.props.router.push({pathname: `/`})
            }

          })
        } else {
          notification.warning({message: "不是一个合法的json对象,请检查", duration: 20,})
        }
      }
    });

  }

  updateRunner = () => {
    let that = this
    const {validateFields, getFieldsValue} =  that.refs.initConfig;
    let formData = getFieldsValue();
    validateFields(null, {}, (err) => {
      if (err) {
        notification.warning({message: "表单校验未通过,请检查", duration: 20,})
        return
      } else {
        if (isJSON(formData.config)) {
          let data = JSON.parse(formData.config);
          putConfigData({name: data.name, body: data}).then(data => {
            if (data === undefined) {
              notification.success({message: "Runner修改成功", duration: 10,})
              this.props.router.push({pathname: `/`})
            }

          })
        } else {
          notification.warning({message: "不是一个合法的json对象,请检查", duration: 20,})
        }
      }
    });

  }

  prev() {
    const current = this.state.current - 1;
    this.setState({current});
  }

  turnToIndex() {
    window.nodeCopy = config.getNodeData()
    this.props.router.push({pathname: `/`})
  }

  render() {
    const {current} = this.state;
    return (
      <div className="logkit-create-container">
        <div className="header">
          <Button style={{float: 'left', marginTop: '20px'}} type="primary" className="index-btn"
                  onClick={() => this.turnToIndex()}>
            <Icon type="link"/>回到首页
          </Button>七牛Logkit配置文件助手 {this.state.version}
        </div>
        <Steps current={current}>
          {steps.map(item => <Step key={item.title} title={item.title}/>)}
        </Steps>
        <div className="steps-content">
          <div className={this.state.current === 0 ? 'show-div' : 'hide-div'}>
            <div>
              <p className={'show-div info'}>根据需要选择需要采集的系统信息类型</p>
            </div>
            <Usages ref="checkUsages"></Usages>
          </div>
          <div className={this.state.current === 1 ? 'show-div' : 'hide-div'}>
            <div>
              <p className={'show-div info'}>选择需要采集的系统信息的字段(某些metric可能无法设置)</p>
            </div>
            <Keys handleMetricKeys={metricKeys=>this.handleMetricKeys(metricKeys)}></Keys>
          </div>
          <div className={this.state.current === 2 ? 'show-div' : 'hide-div'}>
            <div>
              <p className={'show-div info'}>设置Metric的一些配置项(某些metric可能没有配置项)</p>
            </div>
            <Opt handleMetricConfigs={metricConfigs=>this.handleMetricConfigs(metricConfigs)}></Opt>
          </div>
          <div className={this.state.current === 3 ? 'show-div' : 'hide-div'}>
            <div>
              <p className={'show-div info'}>黄色字体选框需根据实际情况修改，其他可作为默认值</p>
            </div>
            <Sender ref="checkSenderData"></Sender>
          </div>
          <div className={this.state.current === 4 ? 'show-div' : 'hide-div'}>
            <RenderConfig ref="initConfig"></RenderConfig>
          </div>

        </div>
        <div className="steps-action">
          {
            this.state.current < steps.length - 1
            &&
            <Button type="primary" onClick={() => this.next()}>下一步</Button>
          }
          {
            this.state.current === steps.length - 1 && this.state.isCopyStatus === false
            &&
            <Button type="primary" onClick={() => this.addRunner()}>确认并提交</Button>
          }
          {
            this.state.current === steps.length - 1 && this.state.isCopyStatus === true
            &&
            <Button type="primary" onClick={() => this.updateRunner()}>修改并提交</Button>
          }
          {
            this.state.current > 0
            &&
            <Button style={{marginLeft: 8}} onClick={() => this.prev()}>
              上一步
            </Button>
          }
        </div>
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
      </div>
    );
  }
}
export default CreateMetricRunner;