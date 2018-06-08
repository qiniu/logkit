import React, {Component} from 'react';
import {
  Form,
  Input,
  Row,
  Col,
  notification,
  InputNumber,
  Select,
  Radio
} from 'antd';
import config from '../store/config'
import moment from 'moment'
import {
  getClusterSlaves,
} from '../services/logkit';
import _ from "lodash";
const Option = Select.Option
const FormItem = Form.Item;
const RadioGroup = Radio.Group

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
    this.notifyJSONError = _.debounce(function(){
      this.notification = notification.warning({message: "不是一个合法的json对象,请检查", duration: 10,})
    }, 500)
  }

  componentDidMount() {

  }

  componentWillUnmount() {
  }

  componentDidUpdate(prevProps) {

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

  handleIntervalChange = (value) => {
    const {getFieldsValue, getFieldDecorator, resetFields} = this.props.form;
    let data = getFieldsValue();
    this.closeNotification();
    if (this.isJSON(data.config)) {
      const jsonData = JSON.parse(data.config)
      jsonData.batch_interval = value
      resetFields()
      getFieldDecorator("config", {initialValue: JSON.stringify(jsonData, null, 2)});
      getFieldDecorator("name", {initialValue: data.name});
      getFieldDecorator("extra_info", {initialValue: data.extra_info});
      getFieldDecorator("collect_interval", {initialValue: parseInt(data.collect_interval)});
      getFieldDecorator("batch_size", {initialValue: parseInt(data.batch_size)});
    } else {
      this.notifyJSONError();
    }
  }
  
  handleBatchSizeChange = (value) => {
    const {getFieldsValue, getFieldDecorator, resetFields} = this.props.form;
    let data = getFieldsValue();
    this.closeNotification();
    if (this.isJSON(data.config)) {
      const jsonData = JSON.parse(data.config)
      jsonData.batch_size = value
      resetFields()
      getFieldDecorator("config", {initialValue: JSON.stringify(jsonData, null, 2)});
      getFieldDecorator("name", {initialValue: data.name});
      getFieldDecorator("extra_info", {initialValue: data.extra_info});
      getFieldDecorator("collect_interval", {initialValue: parseInt(data.collect_interval)});
      getFieldDecorator("batch_interval", {initialValue: parseInt(data.batch_interval)});
    } else {
      this.notifyJSONError();
    }
  }

  handleMetricIntervalChange = (value) => {
    const {getFieldsValue, getFieldDecorator, resetFields} = this.props.form;
    let data = getFieldsValue();
    this.closeNotification();
    if (this.isJSON(data.config)) {
      const jsonData = JSON.parse(data.config)
      jsonData.collect_interval = value
      resetFields()
      getFieldDecorator("config", {initialValue: JSON.stringify(jsonData, null, 2)});
      getFieldDecorator("name", {initialValue: data.name});
      getFieldDecorator("batch_interval", {initialValue: parseInt(data.batch_interval)});
      getFieldDecorator("batch_size", {initialValue: parseInt(data.batch_size)});
      getFieldDecorator("extra_info", {initialValue: data.extra_info});
    } else {
      this.notifyJSONError();
    }

  }

  handleNameChange = (e) => {
    const {getFieldsValue, getFieldDecorator, resetFields} = this.props.form;
    let data = getFieldsValue();
    this.closeNotification();
    if (this.isJSON(data.config)) {
      const jsonData = JSON.parse(data.config)
      jsonData.name = e.target.value
      resetFields()
      getFieldDecorator("config", {initialValue: JSON.stringify(jsonData, null, 2)});
      getFieldDecorator("batch_interval", {initialValue: parseInt(data.batch_interval)});
      getFieldDecorator("batch_size", {initialValue: parseInt(data.batch_size)});
      getFieldDecorator("collect_interval", {initialValue: parseInt(data.collect_interval)});
      getFieldDecorator("extra_info", {initialValue: data.extra_info});
    } else {
      this.notifyJSONError();
    }

  }
  
  handleExtraInfoChange = (e) => {
    const {getFieldsValue, getFieldDecorator, resetFields} = this.props.form;
    let data = getFieldsValue();
    this.closeNotification();
    if (this.isJSON(data.config)) {
      const jsonData = JSON.parse(data.config)
      jsonData.extra_info = e.target.value === 'true'
      resetFields()
      getFieldDecorator("config", {initialValue: JSON.stringify(jsonData, null, 2)});
      getFieldDecorator("batch_interval", {initialValue: parseInt(data.batch_interval)});
      getFieldDecorator("batch_size", {initialValue: parseInt(data.batch_size)});
      getFieldDecorator("collect_interval", {initialValue: parseInt(data.collect_interval)});
      getFieldDecorator("name", {initialValue: data.name});
    } else {
      this.notifyJSONError();
    }
  }

  handleConfigChange = (e) => {
    const {getFieldDecorator, resetFields} = this.props.form;
    this.closeNotification();
    if (this.isJSON(e.target.value)) {
      const jsonData = JSON.parse(e.target.value)
      resetFields()
      getFieldDecorator("name", {initialValue: jsonData.name});
      getFieldDecorator("batch_interval", {initialValue: parseInt(jsonData.batch_interval)});
      getFieldDecorator("collect_interval", {initialValue: parseInt(jsonData.collect_interval)});
      getFieldDecorator("batch_size", {initialValue: parseInt(jsonData.batch_size)});
      getFieldDecorator("extra_info", {initialValue: jsonData.extra_info.toString()});
    } else {
      this.notifyJSONError();
    }

  }
  
  closeNotification(){
    notification.destroy()
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
    const {isMetric} = this.props
    return (
        <div >
          <div className='logkit-body'>
            <Row>
              <Form>
              <FormItem {...formItemLayout} label="名称">
                {getFieldDecorator('name', {rules: [{required: true, message: '名称不能为空'}]})(
                    <Input onChange={this.handleNameChange} placeholder={'收集器(runner)名称'} disabled={window.isCopy ? true : false}/>
                )}
              </FormItem>
              <FormItem {...formItemLayout} 
                label={(<span>最长发送间隔<br/><span style={{ color: 'rgba(0,0,0,.43)', float: 'right' }}>(秒)</span></span>)}>
                  {getFieldDecorator('batch_interval', {
                    rules: [{required: true, message: '发送间隔不能为空'},
                      {pattern: /^[0-9]*$/, message: '输入不符合规范,只能为整数'}]
                  })(
                      <InputNumber onChange={this.handleIntervalChange} placeholder={'发送间隔单位(秒)'}/>
                  )}
                </FormItem>
                {
                  isMetric ? <FormItem {...formItemLayout}
                                                  label={<span>系统信息收集间隔<br /><span style={{ color: 'rgba(0,0,0,.43)', float: 'right' }}>(秒)</span></span>}>
                    {getFieldDecorator('collect_interval', {
                      rules: [{required: true, message: '收集间隔不能为空'},
                        {pattern: /^[0-9]*$/, message: '输入不符合规范,只能为整数'}]
                    })(
                      <InputNumber onChange={this.handleMetricIntervalChange} placeholder={'系统信息收集间隔单位(秒)'}/>
                    )}
                  </FormItem> : null
                }
                <FormItem {...formItemLayout}
                          label={(<span>单次读取最大数据量<br/><span style={{ color: 'rgba(0,0,0,.43)', float: 'right' }}>(byte)</span></span>)}>
                  {getFieldDecorator('batch_size', {
                    rules: [{required: true, message: '单次读取最大数据量不能为空'},
                      {pattern: /^[0-9]*$/, message: '输入不符合规范,只能为整数'}]
                  })(
                    <InputNumber onChange={this.handleBatchSizeChange} />
                  )}
                </FormItem>
                <FormItem {...formItemLayout} label="添加额外系统信息">
                  {getFieldDecorator('extra_info')(
                    <RadioGroup onChange={this.handleExtraInfoChange} style={{float: 'left'}}>
                      <Radio value="true">true</Radio>
                      <Radio value="false">false</Radio>
                    </RadioGroup>
                  )}
                </FormItem>
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