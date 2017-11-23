import React, {Component} from 'react';
import {
  Form,
  Input,
  Row,
  Col,
  notification,
  InputNumber,
  Select
} from 'antd';
import config from '../store/config'
import moment from 'moment'
import {
  getClusterSlaves,
} from '../services/logkit';
import _ from "lodash";
const Option = Select.Option
const FormItem = Form.Item;

const optionFormItemLayout = {
  labelCol: {
    xs: {span: 24},
    sm: {span: 0},
  },
  wrapperCol: {
    xs: {span: 24},
    sm: {span: 24},
  },
};

const formItemLayout = {
  labelCol: {
    xs: {span: 24},
    sm: {span: 6},
  },
  wrapperCol: {
    xs: {span: 24},
    sm: {span: 11},
  },
};

class renderConfig extends Component {
  constructor(props) {
    super(props);
    this.state = {
      current: 0,
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

  computeTagsAndMachine = (items) => {
    window.machineUrls = []
    window.tags = []
    items.forEach((item) => {
      if (!_.includes(window.machineUrl,item.url)) {
        window.machineUrls.push(item.url)
      }
      if (!_.includes(window.tags,item.tag)) {
        window.tags.push(item.tag)
      }
    })
  }

  init = () => {
    let that = this
    if (window.isCluster && window.isCluster === true) {
      getClusterSlaves().then(item => {
        if (item.code === 'L200') {
          that.computeTagsAndMachine(_.values(item.data))
        }
      })
    }

  }


  renderConfigFile = () => {
    const {getFieldDecorator, resetFields} = this.props.form;
    let name = "logkit.runner." + moment().format("YYYYMMDDHHmmss");
    let data = {
      name,
      ...config.getNodeData()
    }
    this.setState({
      currentConfig: JSON.stringify(data, null, 2)
    })
    getFieldDecorator("config", {initialValue: JSON.stringify(data, null, 2)});
    let formData = {}
    formData.config = this.state.currentConfig
    resetFields();
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

  handleIntervalChange = (e) => {
    const {getFieldsValue, getFieldDecorator, resetFields} = this.props.form;
    let data = getFieldsValue();
    if (this.isJSON(data.config)) {
      const jsonData = JSON.parse(data.config)
      jsonData.batch_interval = parseInt(e.target.value)
      resetFields()
      getFieldDecorator("config", {initialValue: JSON.stringify(jsonData, null, 2)});
      getFieldDecorator("name", {initialValue: data.name});
      getFieldDecorator("collect_interval", {initialValue: parseInt(data.collect_interval)});
    } else {
      notification.warning({message: "不是一个合法的json对象,请检查", duration: 10,})
    }

  }

  handleMetricIntervalChange = (e) => {
    const {getFieldsValue, getFieldDecorator, resetFields} = this.props.form;
    let data = getFieldsValue();
    if (this.isJSON(data.config)) {
      const jsonData = JSON.parse(data.config)
      jsonData.collect_interval = parseInt(e.target.value)
      resetFields()
      getFieldDecorator("config", {initialValue: JSON.stringify(jsonData, null, 2)});
      getFieldDecorator("name", {initialValue: data.name});
      getFieldDecorator("batch_interval", {initialValue: parseInt(data.batch_interval)});
    } else {
      notification.warning({message: "不是一个合法的json对象,请检查", duration: 10,})
    }

  }

  handleTagChange = (value) => {
    const {getFieldsValue} = this.props.form;
    let data = getFieldsValue();
    window.tag = ''
    if (this.isJSON(data.config)) {
      window.tag = value
    } else {
      notification.warning({message: "不是一个合法的json对象,请检查", duration: 10,})
    }

  }

  handleMachineChange = (value) => {
    const {getFieldsValue} = this.props.form;
    let data = getFieldsValue();
    window.machine_url = ''
    if (this.isJSON(data.config)) {
      window.machine_url = value
    } else {
      notification.warning({message: "不是一个合法的json对象,请检查", duration: 10,})
    }

  }

  handleNameChange = (e) => {
    const {getFieldsValue, getFieldDecorator, resetFields} = this.props.form;
    let data = getFieldsValue();
    if (this.isJSON(data.config)) {
      const jsonData = JSON.parse(data.config)
      jsonData.name = e.target.value
      resetFields()
      getFieldDecorator("config", {initialValue: JSON.stringify(jsonData, null, 2)});
      getFieldDecorator("batch_interval", {initialValue: parseInt(data.batch_interval)});
      getFieldDecorator("collect_interval", {initialValue: parseInt(data.collect_interval)});
    } else {
      notification.warning({message: "不是一个合法的json对象,请检查", duration: 10,})
    }

  }

  handleConfigChange = (e) => {
    const {getFieldDecorator, resetFields} = this.props.form;
    if (this.isJSON(e.target.value)) {
      const jsonData = JSON.parse(e.target.value)
      resetFields()
      getFieldDecorator("name", {initialValue: jsonData.name});
      getFieldDecorator("batch_interval", {initialValue: parseInt(jsonData.batch_interval)});
      getFieldDecorator("collect_interval", {initialValue: parseInt(jsonData.collect_interval)});
    } else {
      notification.warning({message: "不是一个合法的json对象,请检查", duration: 10,})
    }

  }

  renderSelectOptions = (items) => {
    let options = []
    if (items != undefined) {
      items.map((ele) => {
        options.push(<Option key={ele} value={ele}>{ele}</Option>)
      })
    }
    return (
        options
    )
  }


  render() {
    const {getFieldDecorator} = this.props.form;
    const { currentTagName, currentMachineUrl } = this.props
    if (currentTagName && currentTagName != '') {
      window.tag = currentTagName
    }
    if (currentMachineUrl && currentMachineUrl != '') {
      window.machine_url = currentMachineUrl
    }
    return (
        <div >
          <div className='logkit-body'>
            <Row>
              <Form>
                <FormItem {...formItemLayout} label="名称">
                  {getFieldDecorator('name', {rules: [{required: true, message: '名称不能为空'}]})(
                      <Input onChange={this.handleNameChange} placeholder={'Runner名称'}/>
                  )}
                </FormItem>
                <FormItem {...formItemLayout} label="最长发送间隔(秒)">
                  {getFieldDecorator('batch_interval', {
                    rules: [{required: true, message: '发送间隔不能为空'},
                      {pattern: /^[0-9]*$/, message: '输入不符合规范,只能为整数'}]
                  })(
                      <Input onChange={this.handleIntervalChange} placeholder={'发送间隔单位(秒)'}/>
                  )}
                </FormItem>
                <FormItem {...formItemLayout} label="系统信息收集间隔(metric配置专用, 秒)">
                  {getFieldDecorator('collect_interval', {
                    rules: [{required: true, message: '收集间隔不能为空'},
                      {pattern: /^[0-9]*$/, message: '输入不符合规范,只能为整数'}]
                  })(
                      <Input onChange={this.handleMetricIntervalChange} placeholder={'系统信息收集间隔单位(秒)'}/>
                  )}
                </FormItem>
                {(window.isCluster === true && window.isCopy == false) ? (<div><FormItem {...formItemLayout} label="标签名称">
                  <Select onChange={this.handleTagChange} defaultValue={currentTagName}>
                    {this.renderSelectOptions(window.tags)}
                  </Select>
                </FormItem>
                  <FormItem {...formItemLayout} label="机器地址">
                    <Select onChange={this.handleMachineChange} defaultValue={currentMachineUrl}>
                      {this.renderSelectOptions(window.machineUrls)}
                    </Select>
                  </FormItem></div>) : null}

                <FormItem
                    {...optionFormItemLayout}
                >
                  {getFieldDecorator('config', {
                    rules: [{required: true, message: '配置文件不能为空', trigger: 'blur'}]
                  })(
                      <Input onChange={this.handleConfigChange} type="textarea" rows="50"/>
                  )}
                </FormItem>
              </Form>
            </Row>
          </div>
        </div>
    );
  }
}
export default Form.create()(renderConfig);