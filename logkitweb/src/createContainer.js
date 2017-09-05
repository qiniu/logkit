import React, {Component} from 'react';
import {notification, Button, Steps, Icon} from 'antd';
import Source from  './components/sourceConfig'
import Parser from  './components/parserConfig'
import Sender from './components/senderConfig'
import RenderConfig from './components/renderConfig'
import config from './store/config'
import moment from 'moment'
import {postConfigData} from './services/logkit';

const Step = Steps.Step;
const steps = [{
  title: '配置数据源',
  content: '配置相关数据源信息',
}, {
  title: '配置解析方式',
  content: '配置相关解析方式',
}, {
  title: '配置发送方式',
  content: '配置相关发送方式',
}, {
  title: '确认并添加Runner',
  content: '确认并添加',
}];
class Create extends Component {
  constructor(props) {
    super(props);
    this.state = {
      current: 0,
      isCpoyStatus: false,
      sourceConfigCheck: false,
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
    let isCopy =  this.props.location.query.copyConfig
    if (isCopy === 'true') {
      this.setState({
        current: 3,
        isCpoyStatus: true
      })
      if (window.nodeCopy) {
        that.refs.initConfig.setFieldsValue({config: JSON.stringify(window.nodeCopy, null, 2)});
      }

    }
  }

  next() {
    let that = this;
    this.setState({
      sourceConfigCheck: true
    })
    if (this.state.current === 0) {
      that.refs.checkSourceData.validateFields(null, {}, (err) => {
        if (err) {
          notification.warning({message: "表单校验未通过,请检查", duration: 20,})
        } else {
          const current = this.state.current + 1;
          this.setState({current});
        }
      });
    } else if (this.state.current === 1) {
      that.refs.checkParseData.validateFields(null, {}, (err) => {
        if (err) {
          notification.warning({message: "表单校验未通过,请检查", duration: 20,})
        } else {
          const current = this.state.current + 1;
          this.setState({current});
        }
      });
    } else if (this.state.current === 2) {
      that.refs.checkSenderData.validateFields(null, {}, (err) => {
        if (err) {
          notification.warning({message: "表单校验未通过,请检查", duration: 20,})
        } else {
          const current = this.state.current + 1;
          this.setState({current});
          let name = "logkit.runner." + moment().format("YYYYMMDDHHmmss");
          let data = {
            name,
            ...config.getNodeData()
          }
          that.refs.initConfig.setFieldsValue({config: JSON.stringify(data, null, 2)});
        }
      });
    }

  }

  isJSON = (str) => {
    if (typeof str === 'string') {
      try {
        JSON.parse(str);
        return true;
      } catch (e) {
        return false;
      }
    }
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
        if (this.isJSON(formData.config)) {
          let data = JSON.parse(formData.config);
          postConfigData({name: data.name, body: data}).then(data => {
            if (data === undefined) {
              notification.success({message: "Runner添加成功", duration: 10,})
              this.props.router.push({pathname: `/index`})
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
    this.props.router.push({pathname: `/index`})
  }


  render() {
    const {current} = this.state;
    return (
        <div className="logkit-create-container">
          <div className="header">
            <Button style={{float:'left',marginTop:'15px'}} type="primary" className="index-btn" onClick={() => this.turnToIndex()}>
              <Icon type="link" />回到首页
            </Button>七牛Logkit配置文件助手
          </div>
          <Steps current={current}>
            {steps.map(item => <Step key={item.title} title={item.title}/>)}
          </Steps>
          <div className="steps-content">
            <div><p className={this.state.current <= 2 ? 'show-div info' : 'hide-div'}>注意：黄色字体选框需根据实际情况修改，其他可作为默认值</p>
            </div>
            <div className={this.state.current === 0 ? 'show-div' : 'hide-div'}>
              <Source ref="checkSourceData"></Source>
            </div>
            <div className={this.state.current === 1 ? 'show-div' : 'hide-div'}>
              <Parser ref="checkParseData"></Parser>
            </div>

            <div className={this.state.current === 2 ? 'show-div' : 'hide-div'}>
              <Sender ref="checkSenderData"></Sender>
            </div>

            <div className={this.state.current === 3 ? 'show-div' : 'hide-div'}>
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
              this.state.current === steps.length - 1
              &&
              <Button type="primary" onClick={() => this.addRunner()}>确认并提交</Button>
            }
            {
              this.state.current > 0
              &&
              <Button className={this.state.isCpoyStatus === true ? 'hide': ''} style={{marginLeft: 8}} onClick={() => this.prev()}>
                上一步
              </Button>
            }
          </div>
        </div>
    );
  }
}
export default Create;