import React, {Component} from 'react';
import {
  Form,
  Input,
  Select,
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

class Source extends Component {
  constructor(props) {
    super(props);
    this.state = {
      current: 0,
      items: [],
      options: [],
      currentOption: '',
      currentItem: []
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
    const {getFieldsValue} = this.props.form;
    let data = getFieldsValue();
    let notEmptyKeys = []
    _.forIn(data[this.state.currentOption], function(value,key) {
      if(value != ""){
        notEmptyKeys.push(key)
      }
    });

    config.set('reader', _.pick(data[this.state.currentOption],notEmptyKeys))
  }

  init = () => {

    getSourceOptions().then(data => {
      if (data.success) {
        this.setState({
          options: data,
          currentOption: data[0].key
        })
        getSourceOptionsFormData().then(data => {
          if (data.success) {
            this.setState({
              items: data,
              currentItem: data[this.state.currentOption]
            })
          }
        })
      }
    })


  }

  renderFormItem = () => {
    const {getFieldDecorator} = this.props.form;
    let result = []
    this.state.currentItem.map((ele,index) => {
      if (ele.ChooseOnly == false) {
        result.push(<FormItem key={index}
                              {...formItemLayout}
                              className=""
                              label={(
                                  <span className={ele.DefaultNoUse ? 'warningTip' : '' }>
                  {ele.Description}
                </span>
                              )}>
          {getFieldDecorator(`${this.state.currentOption}.${ele.KeyName}`, {
            initialValue: ele.Default,
            rules: [{required: ele.Default == '' ? false : true, message: '不能为空', trigger: 'blur'},
              {pattern: ele.CheckRegex, message: '输入不符合规范' },
            ]
          })(
              <Input placeholder={ele.DefaultNoUse ? ele.Default : '空值可作为默认值' } disabled={this.state.isReadonly}/>
          )}
        </FormItem>)
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
    const {getFieldDecorator} = this.props.form;
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
            {this.renderFormItem()}
          </Form>
        </div>
    );
  }
}
export default Form.create()(Source);