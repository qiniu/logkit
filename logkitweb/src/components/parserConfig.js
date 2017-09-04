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
      currentOption: 'nginx',
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
    _.forIn(data[this.state.currentOption], function(value,key) {
      if(value != ""){
        notEmptyKeys.push(key)
      }
    });

    config.set('parser', _.pick(data[this.state.currentOption],notEmptyKeys))
  }


  init = () => {
    getSourceParseOptions().then(data => {
      if (data.success) {
        this.setState({
          options: data,
          currentOption: data[0].key
        })
        getSourceParseOptionsFormData().then(data => {
          if (data.success) {
            this.setState({
              items: data,
              currentItem: data.nginx
            })
          }
        })
      }
    })

    getSourceParsesamplelogs().then(data => {
      if (data.success) {
        this.setState({
          sampleData: data,
          currentSampleData: data[this.state.currentOption]
        })
      }
    })


  }

  renderFormItem = () => {
    const {getFieldDecorator} = this.props.form;
    let result = []
    this.state.currentItem.map((ele) => {
      if (ele.ChooseOnly == false) {
        if (ele.KeyName == 'name'){
          ele.Default = "pandora.parser." + moment().format("YYYYMMDDHHmmss");
        }
        result.push(<FormItem
            {...formItemLayout}
            label={(
                <span className={ele.DefaultNoUse ? 'warningTip' : '' }>
                  {ele.Description}
                </span>
            )}>
          {getFieldDecorator(`${this.state.currentOption}.${ele.KeyName}`, {
            initialValue: !ele.DefaultNoUse ? ele.Default : '',
            rules: [{required: ele.Default == '' ? false : true, message: '不能为空', trigger: 'blur'},
              {pattern: ele.CheckRegex, message: '输入不符合规范' },
            ]
          })(
              <Input placeholder={ele.DefaultNoUse ? ele.Default : '空值可作为默认值' } disabled={this.state.isReadonly}/>
          )}
        </FormItem>)
      } else {
        result.push(<FormItem
            {...formItemLayout}
            className=""
            label={ele.Description}>
          {getFieldDecorator(`${this.state.currentOption}.${ele.KeyName}`, {
            initialValue: ele.ChooseOptions[0],
            rules: [{required: true, message: '不能为空', trigger: 'blur'},
              {min: 1, max: 128, message: '长度在 1 到 128 个字符', trigger: 'change'},
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
    this.setState({
      currentOption: option,
      currentItem: this.state.items[option],
      currentSampleData: this.state.sampleData[option]
    })

  }

  renderSelectOptions = () => {
    let options = []
    this.state.options.map((ele) => {
      options.push(<Option value={ele.key}>{ele.value}</Option>)
    })
    return (
        options
    )
  }

  renderChooseOption = (items) => {
    let options = []
    items.map((ele) => {
      options.push(<Option value={ele}>{ele}</Option>)
    })
    return (
        options
    )
  }

  parseSampleData = () => {
    const {getFieldsValue} = this.props.form;
    let data = getFieldsValue();
    const requestData = {
      type: this.state.currentOption,
      ...data[this.state.currentOption],
      sampleLog: this.state.currentSampleData
    }
    postParseData({body: requestData}).then(data => {
      if (data.success) {
        this.setState({
          parseData: JSON.stringify(_.pick(data, 'SamplePoints'), null, 2)
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
            <FormItem {...optionFormItemLayout} label="输入样例日志">
              <Input type="textarea" value={this.state.currentSampleData} rows="6"></Input>
            </FormItem>
            <FormItem {...optionFormItemLayout} label="样例日志">
              <Input type="textarea" value={this.state.parseData} rows="20"></Input>
            </FormItem>
          </Form>
        </div>
    );
  }
}
export default Form.create()(Parser);