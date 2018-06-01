import React, {Component} from 'react';
import {
  Form,
  Input,
  Select,
  Checkbox
} from 'antd';
import {getSenderOptionsFormData, getSenderOptions} from '../services/logkit';
import config from '../store/config'
import moment from 'moment'
import _ from "lodash";

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

class Sender extends Component {
  constructor(props) {
    super(props);
    this.state = {
      current: 0,
      items: [],
      options: [],
      currentOption: '',
      currentItem: [],
      advanceChecked: false
    }
    ;
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
    data[this.state.currentOption].sender_type = this.state.currentOption
    for (const item of this.state.currentItem) {
      if (item.advance_depend && data[this.state.currentOption] && data[this.state.currentOption][item.advance_depend] === 'false') {
        data[this.state.currentOption][item.KeyName] = ''
      }
    }
    let notEmptyKeys = []
    _.forIn(data[this.state.currentOption], function (value, key) {
      if (value != "") {
        notEmptyKeys.push(key)
      }
    });
    config.set('senders', [_.pick(data[this.state.currentOption], notEmptyKeys)])
  }


  init = () => {
    const {setFieldsValue, resetFields} = this.props.form;
    let that = this
    let isMetric = this.props.isMetric
    let trueDefault = ["true", "false"]
    let falseDefault = ["false", "true"]
    getSenderOptions().then(item => {
      if (item.code === 'L200') {
        this.setState({
          options: item.data,
          currentOption: item.data[0].key
        })
        getSenderOptionsFormData().then(item => {
          if (item.code === 'L200') {
            if("pandora" in item.data){
              item.data["pandora"].forEach(function (val, index, arr) {
                if(isMetric === "true"){
                  if(val.KeyName === "pandora_extra_info") {
                    item.data["pandora"][index].ChooseOptions = trueDefault
                  }
                }else{
                  if(val.KeyName === "pandora_extra_info") {
                    item.data["pandora"][index].ChooseOptions = falseDefault
                  }
                }
              })
            }
            this.setState({
              items: item.data,
              currentItem: item.data[this.state.currentOption]
            })
            if (window.nodeCopy) {
              that.handleChange(window.nodeCopy.senders[0].sender_type)
              resetFields();
              let formData = {}
              formData[window.nodeCopy.senders[0].sender_type] = window.nodeCopy.senders[0]
              that.setState({
                currentOption: window.nodeCopy.senders[0].sender_type
              })
              setFieldsValue(formData)
            }
          }
        })
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
      let advanceDependValue = ele.advance_depend && getFieldValue(`${this.state.currentOption}.${ele.advance_depend}`)
      let isAdvanceDependHide = advanceDependValue === 'false' || advanceDependValue === false
      if (ele.ChooseOnly == false) {
        if (ele.KeyName == 'name' && window.isCopy != true) {
          ele.Default = "pandora.sender." + moment().format("YYYYMMDDHHmmss");
        }
        formItem = (
          <FormItem key={index}
                    {...formItemLayout}
                    className={isAdvanceDependHide ? 'hide-div' : 'show-div'}
                    label={labelDes}>
            {getFieldDecorator(`${this.state.currentOption}.${ele.KeyName}`, {
              initialValue: ele.Default,
              rules: [{ required: ele.required && !isAdvanceDependHide , message: '不能为空', trigger: 'blur' },
                { pattern: ele.CheckRegex, message: '输入不符合规范' },
              ]
            })(
              <Input placeholder={ele.DefaultNoUse ? ele.placeholder : '空值可作为默认值'} disabled={this.state.isReadonly} />
            )}
          </FormItem>
        )
      } else {
        formItem = (
          <FormItem key={index}
            {...formItemLayout}
            className={isAdvanceDependHide ? 'hide-div' : 'show-div'}
            label={labelDes}>
            {getFieldDecorator(`${this.state.currentOption}.${ele.KeyName}`, {
              initialValue: ele.Default || ele.ChooseOptions[0],
              rules: [{ required: ele.required && !isAdvanceDependHide, message: '不能为空', trigger: 'blur' },
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
            if (this.state.advanceChecked) {
              result.push(formItem)
            }
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
    this.setState({
      currentOption: option,
      currentItem: this.state.items[option]
    })

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

  render() {
    const {getFieldDecorator} = this.props.form
    const renderResults = this.renderFormItem()
    return (
        <div >
          <Form className="slide-in text-color">
            <FormItem {...optionFormItemLayout} label="选择数据源类型">
              {getFieldDecorator(`${this.state.currentOption}.sender_type`, {
                initialValue: this.state.currentOption
              })(
                  <Select onChange={this.handleChange}>
                    {this.renderSelectOptions()}
                  </Select>)}
            </FormItem>
            <div className="ant-divider ant-divider-horizontal"></div>
            {renderResults.result}
            {
              renderResults.advancedResults.length > 0
                ? (
                  <div>
                    <div className="ant-divider ant-divider-horizontal ant-divider-with-text">
                      <Checkbox onChange={(e) => { this.setState({ advanceChecked: e.target.checked }) }} className="ant-divider-inner-text">高级选项</Checkbox>
                    </div>
                    <div className={this.state.advanceChecked ? 'show-div' : 'hide-div'}>
                      {renderResults.advancedResults}
                    </div>
                  </div>
                )
                : null
            }
          </Form>
        </div>
    );
  }
}
export default Form.create()(Sender);