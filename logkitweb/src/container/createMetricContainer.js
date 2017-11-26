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
import {postConfigData, putConfigData, putClusterConfigData, postClusterConfigData} from '../services/logkit';
import _ from "lodash";

const Step = Steps.Step;
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
    if (window.isCopy === true) {
      this.setState({
        isCopyStatus: true
      })
    }
    if (window.nodeCopy) {
      config.delete("reader");
      config.delete("parser");
      config.delete("transforms");
    }
  }

  next() {
    let that = this;
    if (this.state.current === 0) {
      if (config.get("metric").length > 0) {
        const current = this.state.current + 1;
        this.setState({current});
      } else {
        notification.warning({message: "请至少采集一种系统信息", duration: 20,})
      }
    } else if (this.state.current === 1) {
      let flag = [];
      config.get("metric").map(m => {
        let isHasTrue = false;
        for (let k in this.state.metricKeys[m.type]) {
          if (this.state.metricKeys[m.type][k]) {
            isHasTrue = true;
            return true;
          }
        }
        if (!isHasTrue && Object.keys(this.state.metricKeys[m.type]).length) flag.push(m.type);
      });
      if (flag.length <= 0) {
        const current = this.state.current + 1;
        this.setState({current});
      } else {
        notification.warning({message: "请至少为" + flag.join(", ") + "选择一个采集的字段", duration: 20,})
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
            runnerName = window.nodeCopy.name
            batch_interval = window.nodeCopy.batch_interval
            collect_interval = window.nodeCopy.collect_interval
          }
          let data = {
            name: runnerName != undefined ? runnerName : name,
            batch_interval: batch_interval != undefined ? batch_interval : 60,
            collect_interval: collect_interval != undefined ? collect_interval : 3,
            ...config.getNodeData()
          }
          that.refs.initConfig.setFieldsValue({config: JSON.stringify(data, null, 2)});
          that.refs.initConfig.setFieldsValue({name: runnerName != undefined ? runnerName : name});
          that.refs.initConfig.setFieldsValue({batch_interval: batch_interval != undefined ? batch_interval : 60});
          that.refs.initConfig.setFieldsValue({collect_interval: collect_interval != undefined ? collect_interval : 3});
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
    if (!metric) return;
    metric.map((m, _) => {
      m["attributes"] = this.state.metricKeys[m.type];
      m["config"] = this.state.metricConfigs[m.type];
      configData.push(m);
    });
    config.set("metric", configData);
  }

  addRunner = () => {
    const { currentTagName, currentMachineUrl } = this.props
    const {handleTurnToRunner} = this.props
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
          let tag = (currentTagName != null && currentTagName != undefined) ? currentTagName : ''
          let url = (currentMachineUrl != null && currentMachineUrl != undefined) ? currentMachineUrl : ''
          if (window.isCluster && window.isCluster === true) {
            postClusterConfigData({name: data.name, tag: tag, url: url, body: data}).then(data => {
              if (data && data.code === 'L200') {
                notification.success({message: "Metric Runner添加成功", duration: 10,})
                handleTurnToRunner()
              }

            })
          } else {
            postConfigData({name: data.name, body: data}).then(data => {
              if (data && data.code === 'L200') {
                notification.success({message: "Metric Runner添加成功", duration: 10,})
                handleTurnToRunner()
              }

            })
          }
        } else {
          notification.warning({message: "不是一个合法的json对象,请检查", duration: 20,})
        }
      }
    });

  }

  updateRunner = () => {
    const { currentTagName, currentMachineUrl } = this.props
    const {handleTurnToRunner} = this.props
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
          let tag = (currentTagName != null && currentTagName != undefined) ? currentTagName : ''
          let url = (currentMachineUrl != null && currentMachineUrl != undefined) ? currentMachineUrl : ''
          if (window.isCluster && window.isCluster === true) {
            putClusterConfigData({name: data.name, tag: tag, url: url, body: data}).then(data => {
              if (data && data.code === 'L200') {
                notification.success({message: "Runner修改成功", duration: 10,})
                handleTurnToRunner()
              }

            })
          } else {
            putConfigData({name: data.name, body: data}).then(data => {
              if (data && data.code === 'L200') {
                notification.success({message: "Runner修改成功", duration: 10,})
                handleTurnToRunner()
              }

            })
          }
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
              <Keys handleMetricKeys={metricKeys => this.handleMetricKeys(metricKeys)}></Keys>
            </div>
            <div className={this.state.current === 2 ? 'show-div' : 'hide-div'}>
              <div>
                <p className={'show-div info'}>设置Metric的一些配置项(某些metric可能没有配置项)</p>
              </div>
              <Opt handleMetricConfigs={metricConfigs => this.handleMetricConfigs(metricConfigs)}></Opt>
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
        </div>
    );
  }
}
export default CreateMetricRunner;