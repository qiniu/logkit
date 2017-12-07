import React, {Component} from 'react';
import {
  Form,
  Input,
  Select,
  Button,
} from 'antd';
import _ from "lodash";
import config from '../store/config'
import moment from 'moment'
import {
  getSourceParseOptionsFormData,
  getSourceParseOptions,
  getSourceParsesamplelogs,
  postParseData
} from '../services/logkit';

const Option = Select.Option
const FormItem = Form.Item;

const formItemLayout = {
  labelCol: {
    xs: {span: 24},
    sm: {span: 7},
  },
  wrapperCol: {
    xs: {span: 24},
    sm: {span: 10},
  },
};

const optionFormItemLayout = {
  labelCol: {
    xs: {span: 24},
    sm: {span: 7},
  },
  wrapperCol: {
    xs: {span: 24},
    sm: {span: 10},
  },
};

class Parser extends Component {
  constructor(props) {
    super(props);
    this.state = {
      current: 0,
      items: [],
      options: [],
      currentOption: '',
      currentItem: [],
      parseData: '',
      sampleData: [],
      currentSampleData: ''
    };
  }

  componentDidMount() {
    this.init()
  }

  componentWillUnmount() {
  }

  componentDidUpdate(prevProps) {
    this.submit()
  }

  submit = () => {
    const {getFieldsValue} = this.props.form;
    let data = getFieldsValue();
    let notEmptyKeys = []
    _.forIn(data[this.state.currentOption], function (value, key) {
      if (value !== "") {
        notEmptyKeys.push(key)
      }
    });

    config.set('parser', _.pick(data[this.state.currentOption], notEmptyKeys))
  }


  init = () => {
    const {getFieldDecorator, setFieldsValue, resetFields} = this.props.form;
    let that = this
    getSourceParseOptions().then(item => {
      if (item.code === 'L200') {
        this.setState({
          options: item.data,
          currentOption: item.data[0].key
        })
        getSourceParseOptionsFormData().then(item => {
          if (item.code === 'L200') {
            this.setState({
              items: item.data,
              currentItem: item.data[this.state.currentOption]
            })

            if (window.nodeCopy) {
              that.handleChange(window.nodeCopy.parser.type)
              resetFields();
              let formData = {}
              formData[window.nodeCopy.parser.type] = window.nodeCopy.parser
              that.setState({
                currentOption: window.nodeCopy.parser.type,
              })
              setFieldsValue(formData);
            }
          }
        })
      }

    })

    getSourceParsesamplelogs().then(item => {
      if (item.code === 'L200') {
        this.setState({
          sampleData: item.data,
          currentSampleData: item.data[this.state.currentOption]
        })
        getFieldDecorator("sampleData", {initialValue: item.data[this.state.currentOption]});
      }
    })


  }

  renderFormItem = () => {
    const {getFieldDecorator} = this.props.form;
    let result = []
    this.state.currentItem.map((ele, index) => {
      if (ele.ChooseOnly == false) {
        if (ele.KeyName == 'name' && window.isCopy != true) {
          ele.Default = "pandora.parser." + moment().format("YYYYMMDDHHmmss");
        }
        if (ele.KeyName === 'grok_custom_patterns') {
          result.push(<FormItem key={index}
                                {...formItemLayout}
                                label={(
                                    <span className={ele.DefaultNoUse ? 'warningTip' : '' }>
                  {ele.Description}
                </span>
                                )}>
            {getFieldDecorator(`${this.state.currentOption}.${ele.KeyName}`, {
              initialValue: !ele.DefaultNoUse ? ele.Default : '',
              rules: [{required: ele.Default == '' ? false : true, message: '不能为空', trigger: 'blur'},
                {pattern: ele.CheckRegex, message: '输入不符合规范'},
              ]
            })(
                <Input type="textarea" rows="6" placeholder={ele.DefaultNoUse ? ele.Default : '空值可作为默认值' }
                       disabled={this.state.isReadonly}/>
            )}
          </FormItem>)
        } else {
          result.push(<FormItem key={index}
                                {...formItemLayout}
                                label={(
                                    <span className={ele.DefaultNoUse ? 'warningTip' : '' }>
                  {ele.Description}
                </span>
                                )}>
            {getFieldDecorator(`${this.state.currentOption}.${ele.KeyName}`, {
              initialValue: ele.Default,
              rules: [{required: ele.Default == '' ? false : true, message: '不能为空', trigger: 'blur'},
                {pattern: ele.CheckRegex, message: '输入不符合规范'},
              ]
            })(
                <Input placeholder={ele.DefaultNoUse ? ele.Default : '空值可作为默认值' } disabled={this.state.isReadonly}/>
            )}
          </FormItem>)
        }

      } else {
        result.push(<FormItem key={index}
                              {...formItemLayout}
                              className=""
                              label={ele.Description}>
          {getFieldDecorator(`${this.state.currentOption}.${ele.KeyName}`, {
            initialValue: ele.ChooseOptions[0],
            rules: [{required: true, message: '不能为空', trigger: 'blur'},
            ]
          })(
              <Select>
                {this.renderChooseOption(ele.ChooseOptions)}
              </Select>
          )}
        </FormItem>)
      }

    })
    return (
        result
    )

  }

  handleChange = (option) => {
    const {getFieldDecorator,resetFields} = this.props.form;
    this.setState({
      currentOption: option,
      currentItem: this.state.items[option],
      currentSampleData: this.state.sampleData[option]
    })
    this.setState({
      parseData: ''
    })
    resetFields()
    getFieldDecorator("sampleData", {initialValue: this.state.sampleData[option]});
  }

  renderSelectOptions = () => {
    let options = []
    this.state.options.map((ele) => {
      options.push(<Option key={ele.key} value={ele.key}>{ele.value}</Option>)
    })
    return (
        options
    )
  }

  renderChooseOption = (items) => {
    let options = []
    items.map((ele) => {
      options.push(<Option key={ele} value={ele}>{ele}</Option>)
    })
    return (
        options
    )
  }

  parseSampleData = () => {
    const {getFieldsValue} = this.props.form;
    let data = getFieldsValue();
    if (this.state.currentOption === 'grok') {
      if (data[this.state.currentOption].grok_custom_patterns != '' && data[this.state.currentOption].grok_custom_patterns != undefined) {
        data[this.state.currentOption].grok_custom_patterns = window.btoa(data[this.state.currentOption].grok_custom_patterns)
      }
    }
    const requestData = {
      type: this.state.currentOption,
      ...data[this.state.currentOption],
      sampleLog: data.sampleData
    }
    postParseData({body: requestData}).then(item => {
      if (item.code === 'L200') {
        this.setState({
          parseData: JSON.stringify(_.pick(item.data, 'SamplePoints'), null, 2)
        })
      }

    })
  }

  render() {
    const {getFieldDecorator} = this.props.form;
    return (
        <div >
          <Form className="slide-in text-color">
            <FormItem {...optionFormItemLayout} label="选择解析方式">
              {getFieldDecorator(`${this.state.currentOption}.type`, {
                initialValue: this.state.currentOption
              })(
                  <Select onChange={this.handleChange}>
                    {this.renderSelectOptions()}
                  </Select>)}
            </FormItem>
            {this.renderFormItem()}
            <FormItem {...optionFormItemLayout} >
              <Button type="primary" onClick={this.parseSampleData}>解析样例数据</Button>
            </FormItem>
            <FormItem
                label={'输入样例日志'}
                {...optionFormItemLayout}
            >
              {getFieldDecorator('sampleData', {
                rules: []
              })(
                  <Input type="textarea" rows="8"/>
              )}
            </FormItem>
            <FormItem {...optionFormItemLayout} label="样例日志">
              <Input type="textarea" value={this.state.parseData} rows="15"></Input>
            </FormItem>
          </Form>
        </div>
    );
  }
}
export default Form.create()(Parser);