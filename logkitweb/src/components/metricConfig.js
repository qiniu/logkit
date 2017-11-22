import React, {Component} from 'react';
import {
  Form,
  Input,
  Select,
} from 'antd';
import config from '../store/config'
import {getMetricOptions} from '../services/logkit';

const Option = Select.Option;
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

class Opt extends Component {
  constructor(props) {
    super(props);
    this.state = {
      items: {},
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
    let data = this.state.items;
    let formData = getFieldsValue();
    let metricConfigs = {};
    for (let m in formData) {
      metricConfigs[m] = {};
      data[m].map(c => {
        if (c.Type === "bool") {
          metricConfigs[m][c.KeyName] = formData[m][c.KeyName] === "true";
        } else if (c.Type === "array") {
          let tmp = formData[m][c.KeyName].split(/\s*,\s*/);
          metricConfigs[m][c.KeyName] = [];
          tmp.map(i => {
            if (i.length > 0) {
              metricConfigs[m][c.KeyName].push(i);
            }
          });
        } else {
          metricConfigs[m][c.KeyName] = formData[m][c.KeyName];
        }
      });
    }
    this.props.handleMetricConfigs(metricConfigs);
  }

  init = () => {
    getMetricOptions().then(item => {
      if (item.code === 'L200') {
        const {setFieldsValue} = this.props.form;
        this.setState({
          items: item.data,
        });
        if (window.nodeCopy && window.nodeCopy.metric) {
          let formData = {};
          window.nodeCopy.metric.map(m => {
            formData[m.type] = {};
            item.data[m.type].map(c => {
              if (m.config && m.config[c.KeyName] !== undefined) {
                if (c.Type === "bool") {
                  formData[m.type][c.KeyName] = m.config[c.KeyName].toString();
                } else if (c.Type === "array") {
                  formData[m.type][c.KeyName] = m.config[c.KeyName].join(',');
                } else {
                  formData[m.type][c.KeyName] = m.config[c.KeyName];
                }
              }
            });
          });
          setFieldsValue(formData);
        }
      }

    })
  }

  renderFormItem = () => {
    let result = [];
    let selectedMetric = config.get("metric");
    if (!selectedMetric) selectedMetric = [];
    const {getFieldDecorator} = this.props.form;
    selectedMetric.map((metric, _) => {
      let key = metric.type;
      let configs = this.state.items[key];
      if (!configs || configs.length <= 0) {
        return true;
      }
      result.push(
          <FormItem key={key}>
            <b>{key} 配置项</b>
            <hr/>
          </FormItem>
      );
      configs.map((ele, index) => {
        if (ele.ChooseOnly == false) {
          result.push(<FormItem key={ele.KeyName}
                                {...formItemLayout}
                                className=""
                                label={(
                                    <span className={ele.DefaultNoUse ? 'warningTip' : '' }>
                  {ele.Description}
                </span>
                                )}>
            {getFieldDecorator(`${key}.${ele.KeyName}`, {
              initialValue: ele.Default,
              rules: [{required: ele.Default == '' ? false : true, message: '不能为空', trigger: 'blur'},
                {pattern: ele.CheckRegex, message: '输入不符合规范'},
              ]
            })(
                <Input placeholder={ele.DefaultNoUse ? ele.Default : '空值可作为默认值' } disabled={this.state.isReadonly}/>
            )}
          </FormItem>)
        } else {
          result.push(<FormItem key={ele.KeyName}
                                {...formItemLayout}
                                className=""
                                label={ele.Description}>
            {getFieldDecorator(`${key}.${ele.KeyName}`, {
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
      });
    });
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
export default Form.create()(Opt);