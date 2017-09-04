import React, {Component} from 'react';
import {
  Form,
  Input,
  Select
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
      currentOption: 'pandora',
      currentItem: []
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
    let notEmptyKeys = []
    _.forIn(data[this.state.currentOption], function(value,key) {
      if(value != ""){
        notEmptyKeys.push(key)
      }
    });
    config.set('senders', [_.pick(data[this.state.currentOption],notEmptyKeys)])
  }


  init = () => {

    getSenderOptions().then(data => {
      if (data.success) {
        this.setState({
          options: data,
          currentOption: data[0].key
        })
        getSenderOptionsFormData().then(data => {
          if (data.success) {
            this.setState({
              items: data,
              currentItem: data.pandora
            })
          }
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
          ele.Default = "pandora.sender." + moment().format("YYYYMMDDHHmmss");
        }
        result.push(<FormItem
            {...formItemLayout}
            className=""
            label={(
                <span className={ele.DefaultNoUse ? 'warningTip' : '' }>
                  {ele.Description}
                </span>
            )}>
          {getFieldDecorator(`${this.state.currentOption}.${ele.KeyName}`, {
            initialValue: !ele.DefaultNoUse ? ele.Default : '',
            rules: [{required: ele.Default == '' ? false : true, message: '不能为空', trigger: 'blur'},
              {min: 1, max: 128, message: '长度在 1 到 128 个字符', trigger: 'change'},
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
      currentItem: this.state.items[option]
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

  render() {
    const {getFieldDecorator} = this.props.form;
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
            {this.renderFormItem()}
          </Form>
        </div>
    );
  }
}
export default Form.create()(Sender);