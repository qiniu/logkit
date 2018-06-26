import React, {Component} from 'react';
import {
  Form,
  Input,
  Select,
  Checkbox,
  InputNumber
} from 'antd';
import config from '../store/config'
import _ from "lodash";
import {getSourceOptionsFormData, getSourceOptions} from '../services/logkit';

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

const allowDeleteOption = ['file', 'fileauto', 'dir', 'tailx']
const cleanerConfig = [{
  KeyName: 'delete_enable',
  Default: 'false',
  Label: '是否自动删除日志文件'
}, {
  KeyName: 'delete_interval',
  Default: '10',
  Label: '删除执行周期',
  Unit: '秒'
}, {
  KeyName: 'reserve_file_number',
  Default: '10',
  Label: '最大保留已读文件数'
}, {
  KeyName: 'reserve_file_size',
  Default: '2048',
  Label: '最大保留已读文件总大小',
  Unit: 'MB'
}]

class Source extends Component {
  constructor(props) {
    super(props);
    this.state = {
      current: 0,
      items: [],
      options: [],
      currentOption: '',
      currentItem: [],
      advanceChecked: false,
      enableDelete: false
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

  componentWillReceiveProps(nextProps) {

  }

  submit = () => {
    const {getFieldsValue} = this.props.form
    const cleanerKeys = cleanerConfig.map(item => item.KeyName)
    let data = getFieldsValue()
    let notEmptyKeysReader = []
    let notEmptyKeysCleaner = []
    for (const item of this.state.currentItem) {
      if (item.advance_depend && data[this.state.currentOption] && data[this.state.currentOption][item.advance_depend] === 'false') {
        data[this.state.currentOption][item.KeyName] = ''
      }
    }
    _.forIn(data[this.state.currentOption], function (value, key) {
      if (value !== "") {
        cleanerKeys.includes(key) ? notEmptyKeysCleaner.push(key) : notEmptyKeysReader.push(key)
      }
    });
    let cleanerData = _.pick(data[this.state.currentOption], notEmptyKeysCleaner)
    if (cleanerData && typeof cleanerData['delete_enable'] === 'boolean') {
      if (cleanerData.delete_enable) {
        cleanerData.delete_enable = cleanerData.delete_enable.toString()
      } else {
        cleanerData = {}
      }
      console.log(cleanerData)
    }

    config.set('reader', _.pick(data[this.state.currentOption], notEmptyKeysReader))
    config.set('cleaner', cleanerData)
  }

  init = () => {
    const {setFieldsValue, resetFields} = this.props.form;
    let that = this

    getSourceOptions().then(item => {
      if (item.code === 'L200') {
        this.setState({
          options: item.data,
          currentOption: item.data[0].key
        })
        getSourceOptionsFormData().then(item => {
          if (item.code === 'L200') {
            this.setState({
              items: item.data,
              currentItem: item.data[this.state.currentOption]
            })

            if (window.nodeCopy) {
              that.handleChange(window.nodeCopy.reader.mode)
              resetFields();
              let formData = {}
              formData[window.nodeCopy.reader.mode] = {...window.nodeCopy.reader, ...window.nodeCopy.cleaner}
              that.setState({
                currentOption: window.nodeCopy.reader.mode
              })
              setFieldsValue(formData);
            }
          }
        })
      }

    })
  }

  renderFormItem = () => {
    const { getFieldDecorator, getFieldValue } = this.props.form
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
        formItem = (
          <FormItem key={index}
                    {...formItemLayout}
                    className={isAdvanceDependHide ? 'hide-div' : 'show-div'}
                    label={labelDes}>
            {getFieldDecorator(`${this.state.currentOption}.${ele.KeyName}`, {
              initialValue: ele.Default,
              rules: [{ required: ele.required && !isAdvanceDependHide, message: '不能为空', trigger: 'blur' },
                { pattern: ele.CheckRegex, message: '输入不符合规范' },
              ]
            })(
              ele.Element === 'text'
                ? <Input.TextArea placeholder={ele.DefaultNoUse ? ele.placeholder : '空值可作为默认值'} disabled={this.state.isReadonly} />
                : <Input placeholder={ele.DefaultNoUse ? ele.placeholder : '空值可作为默认值'} disabled={this.state.isReadonly} />
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
  
  renderExtraFormItem = () => {
    const {currentOption} = this.state
    const {getFieldDecorator, getFieldValue, setFieldsValue} = this.props.form
    if (allowDeleteOption.includes(currentOption)) {
      const cleanerChildConfig = cleanerConfig.filter(item => item.KeyName !== 'delete_enable')
      const enableDelete = getFieldValue(`${this.state.currentOption}.delete_enable`)
      return (
        <div>
          <FormItem
                    {...formItemLayout}
                    className="delete_enable"
                    label={<span>是否自动删除日志文件<br /></span>}>
            {getFieldDecorator(`${this.state.currentOption}.delete_enable`, {
              initialValue: false
            })(
              <Checkbox checked={!!enableDelete}>自动删除</Checkbox>
            )}
          </FormItem>
          <div className={enableDelete ? 'show-div': 'hide-div'}>
            {cleanerChildConfig.map((item) => (
              <FormItem
                key={item.KeyName}
                {...formItemLayout}
                className="delete-enable-input-number"
                label={<span>{item.Label}<br /></span>}>
                {getFieldDecorator(`${this.state.currentOption}.${item.KeyName}`, {
                  initialValue: item.Default
                })(
                  <Input />
                )}
                {item.Unit}
              </FormItem>
            ))}
          </div>
        </div>
      )
    }
  }

  render() {
    const {getFieldDecorator} = this.props.form
    const renderResults = this.renderFormItem()
    return (
        <div>
          <Form className="slide-in text-color">
            <FormItem {...optionFormItemLayout} label="选择数据源类型">
              {getFieldDecorator(`${this.state.currentOption}.mode`, {
                initialValue: this.state.currentOption
              })(
                  <Select onChange={this.handleChange}>
                    {this.renderSelectOptions()}
                  </Select>)}
            </FormItem>
            <div className="ant-divider ant-divider-horizontal"></div>
            {renderResults.result}
            {this.renderExtraFormItem()}
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
export default Form.create()(Source);