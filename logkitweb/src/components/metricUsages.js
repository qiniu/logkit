import React, {Component} from 'react';
import {
  Form,
  Input,
  Select,
} from 'antd';
import _ from "lodash";
import config from '../store/config'
import {getMetricUsages} from '../services/logkit';

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

class Usages extends Component {
  constructor(props) {
    super(props);
    this.state = {
      items: [],
    };
  }

  componentDidMount() {
    this.init()
  }

  componentWillUnmount() {
  }

  componentDidUpdate(prevProps) {
    this.submit();
  }

  init = () => {
    getMetricUsages().then(item => {
      if (item.code === 'L200') {
        const {setFieldsValue} = this.props.form;
        this.setState({
          items: item.data,
        })
        if (window.nodeCopy && window.nodeCopy.metric) {
          let formData = {};
          window.nodeCopy.metric.map((m, _) => {
            formData[m.type] = "true";
          });
          item.data.map((m, _) => {
            if (formData[m.KeyName] === undefined) {
              formData[m.KeyName] = "false";
            }
          });
          setFieldsValue(formData);
        }
      }
    })
  }

  submit = () => {
    let selectedMetric = [];
    const {getFieldsValue} = this.props.form;
    let data = getFieldsValue();
    _.forIn(data, function (value, key) {
      if (value === "true") {
        selectedMetric.push({"type": key});
      }
    });
    config.set('metric', selectedMetric);
  }

  renderFormItem = () => {
    let result = []
    const {getFieldDecorator} = this.props.form;
    this.state.items.map((ele, index) => {
      if (ele.ChooseOnly == false) {
        result.push(<FormItem key={index}
                              KeyName={ele.KeyName}
                              {...formItemLayout}
                              className=""
                              label={(
                                  <span className={ele.DefaultNoUse ? 'warningTip' : '' }>
                  {ele.Description}
                </span>
                              )}>
          {getFieldDecorator(`${ele.KeyName}`, {
            initialValue: ele.Default,
            rules: [{required: ele.Default == '' ? false : true, message: '不能为空', trigger: 'blur'},
              {pattern: ele.CheckRegex, message: '输入不符合规范'},]
          })(
              <Input placeholder={ele.DefaultNoUse ? ele.Default : '空值可作为默认值' } disabled={this.state.isReadonly}/>
          )}
        </FormItem>)
      } else {
        result.push(<FormItem key={index}
                              KeyName={ele.KeyName}
                              {...formItemLayout}
                              className=""
                              label={ele.Description}>
          {getFieldDecorator(`${ele.KeyName}`, {
            initialValue: ele.ChooseOptions[0],
            rules: [{required: true, message: '不能为空', trigger: 'blur'},]
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
    return (
        <div>
          <Form className="slide-in text-color">
            {this.renderFormItem()}
          </Form>
        </div>
    );
  }
}
export default Form.create()(Usages);