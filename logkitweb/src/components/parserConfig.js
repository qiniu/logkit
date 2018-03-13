import React, {Component} from 'react';
import {
  Form,
  Input,
  Select,
  Button,
  Checkbox
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
    const { getFieldDecorator, getFieldValue } = this.props.form;
    let result = []
    let advancedResults = []
    this.state.currentItem.map((ele, index) => {
      let formItem = null
      const labelDes = (
        <span>
          {ele.Description.slice(0, ele.Description.indexOf('('))}
          <br />
          <span style={{ color: 'rgba(0,0,0,.43)', float: 'right' }}>
            {ele.Description.slice(ele.Description.indexOf('('), ele.Description.length)}
          </span>
        </span>
      )
      if (ele.ChooseOnly == false) {
        if (ele.KeyName == 'name' && window.isCopy != true) {
          ele.Default = "pandora.parser." + moment().format("YYYYMMDDHHmmss");
        }
        if (ele.KeyName === 'grok_custom_patterns') {
          formItem = (
            <FormItem key={index}
              {...formItemLayout}
              label={labelDes}>
              {getFieldDecorator(`${this.state.currentOption}.${ele.KeyName}`, {
                initialValue: !ele.DefaultNoUse ? ele.Default : '',
                rules: [{ required: ele.Default == '' ? false : true, message: '不能为空', trigger: 'blur' },
                { pattern: ele.CheckRegex, message: '输入不符合规范' },
                ]
              })(
                <Input type="textarea" rows="6" placeholder={ele.DefaultNoUse ? ele.Default : '空值可作为默认值'}
                  disabled={this.state.isReadonly} />
                )}
            </FormItem>
          )
        } else {
          formItem = (
            <FormItem key={index}
              {...formItemLayout}
              label={labelDes}>
              {getFieldDecorator(`${this.state.currentOption}.${ele.KeyName}`, {
                initialValue: ele.Default,
                rules: [{ required: ele.Default == '' ? false : true, message: '不能为空', trigger: 'blur' },
                { pattern: ele.CheckRegex, message: '输入不符合规范' },
                ]
              })(
                <Input placeholder={ele.DefaultNoUse ? ele.Default : '空值可作为默认值'} disabled={this.state.isReadonly} />
                )}
            </FormItem>
          )
        }
        if (ele.advance_depend && getFieldValue(`${this.state.currentOption}.${ele.advance_depend}`) === 'false') {
          formItem = null
        }
      } else {
        formItem = (
          <FormItem key={index}
            {...formItemLayout}
            className=""
            label={labelDes}>
            {getFieldDecorator(`${this.state.currentOption}.${ele.KeyName}`, {
              initialValue: ele.Default || ele.ChooseOptions[0],
              rules: [{ required: true, message: '不能为空', trigger: 'blur' },
              ]
            })(
              <Select>
                {this.renderChooseOption(ele.ChooseOptions)}
              </Select>
              )}
          </FormItem>
        )
      }
      if (ele && ele.advance) {
        if (!ele.advance_depend) {
          advancedResults.push(formItem)
        } else {
          const advancedItem = this.getAdvancedConfig(ele)
          if (advancedItem && advancedItem.advance) {
            advancedResults.push(formItem)
          } else {
            result.push(formItem)
          }
        }
      } else {
        result.push(formItem)
      }
    })
    return (
      {
        result,
        advancedResults
      }
    )
  }

  getAdvancedConfig = (ele) => {
    if (ele.advance_depend) {
      const dependItem = this.state.currentItem.find((item) => item.KeyName === ele.advance_depend)
      return dependItem
    }
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
    const {getFieldDecorator} = this.props.form
    const renderResults = this.renderFormItem()
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
            <div className="form-item-underline"></div>
            {renderResults.result}
            {
              renderResults.advancedResults.length > 0
              ? (
                <div>
                  <div className="form-item-advance-checkbox">
                    <div className="form-item-advance-decorator-left"></div>
                    <Checkbox onChange={(e) => { this.setState({ advanceChecked: e.target.checked }) }}>高级选项</Checkbox>
                    <div className="form-item-advance-decorator-right"></div>
                  </div>
                  {this.state.advanceChecked ? renderResults.advancedResults : null}
                </div>
              )
              : null
            }
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