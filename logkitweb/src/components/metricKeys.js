import React, {Component} from 'react';
import {
  Form,
  Row,
  Col,
  Checkbox,
} from 'antd';
import config from '../store/config'
import {getMetricKeys} from '../services/logkit';

const FormItem = Form.Item;

class Keys extends Component {
  constructor(props) {
    super(props);
    this.state = {
      items: {},
      isChecked: {},
      indeterminate: {},
      checkAll: {},
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
    this.props.handleMetricKeys(this.state.isChecked);
  }

  init = () => {
    getMetricKeys().then(item => {
      if (item.code === 'L200') {
        const isChecked = {}
        Object.keys(item.data).filter(key => key !== 'success').forEach(
            key => {
              const child = {}
              item.data[key].forEach(
                  item => {
                    child[item.key] = true;
                  }
              )
              isChecked[key] = child;
            }
        )
        if (window.nodeCopy && window.nodeCopy.metric) {
          window.nodeCopy.metric.map(m => {
            for (let k in m.attributes) {
              isChecked[m.type][k] = m.attributes[k];
            }
          });
        }
        this.setState({
          items: item.data,
          isChecked: isChecked,
        });
        this.initCheckAll();
      }
    })
  }

  initCheckAll = () => {
    let checkAll = {};
    let indeterminate = {};
    let data = this.state.items;
    for (let m in this.state.isChecked) {
      let falseCnt = 0;
      for (let k in this.state.isChecked[m]) {
        if (!this.state.isChecked[m][k]) {
          falseCnt++;
        }
      }
      if (falseCnt === 0) {
        checkAll[m] = true;
        indeterminate[m] = false;
      } else if (falseCnt === data[m].length) {
        checkAll[m] = false;
        indeterminate[m] = false;
      } else {
        checkAll[m] = false;
        indeterminate[m] = true;
      }
    }
    this.setState({
      checkAll: checkAll,
      indeterminate: indeterminate,
    })
  }

  onCheckboxChange = (m, k) => {
    let isChecked = this.state.isChecked;
    isChecked[m][k] = !isChecked[m][k];
    this.setState({
      isChecked: isChecked,
    });
    this.initCheckAll();
  }

  onCheckAllChange = (key) => {
    let checkAll = this.state.checkAll;
    let isChecked = this.state.isChecked;
    let indeterminate = this.state.indeterminate;
    if (checkAll[key]) {
      for (let k in isChecked[key]) {
        isChecked[key][k] = false;
      }
      indeterminate[key] = false;
      checkAll[key] = false;
    } else {
      for (let k in isChecked[key]) {
        isChecked[key][k] = true;
      }
      indeterminate[key] = false;
      checkAll[key] = true;
    }
    this.setState({
      checkAll: checkAll,
      isChecked: isChecked,
      indeterminate: indeterminate,
    })
  }

  renderFormItem = () => {
    let result = [];
    let isChecked = this.state.isChecked;
    let selectedMetric = config.get("metric");
    if (!selectedMetric) selectedMetric = [];
    const {getFieldDecorator} = this.props.form;
    selectedMetric.map(metric => {
      let key = metric.type;
      let ele = this.state.items[key];
      if (!ele || ele.length <= 0) {
        return true;
      }
      result.push(
          <FormItem key={key}>
            <dis>
              <span><b>{key} 字段配置</b></span>
              <span style={{marginLeft: "26px"}}>
              <Checkbox
                  key={key}
                  indeterminate={this.state.indeterminate[key]}
                  onChange={() => {
                    this.onCheckAllChange(key)
                  }}
                  checked={this.state.checkAll[key]}
              >全选/全不选</Checkbox>
            </span>
              <hr/>
            </dis>
            <Row style={{textAlign: "left", paddingLeft: "26px"}}>
              {ele.map(i => (
                  getFieldDecorator(`${key}.${i.key}`, {valuePropName: 'checked', initialValue: isChecked[key][i.key]})(
                      <Col span={12} key={i.key}>
                        <Checkbox checked={isChecked[key][i.key]} onChange={() => this.onCheckboxChange(key, i.key)}>
                          {i.key + " (" + i.value + ")"}
                        </Checkbox>
                      </Col>
                  )
              ))}
            </Row>
          </FormItem>
      );
    });
    return (
        result
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
export default Form.create()(Keys);