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
      checkedList: {},
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
    const {getFieldsValue} = this.props.form;
    let data = getFieldsValue();
    this.props.handleMetricKeys(data);
  }

  init = () => {
    getMetricKeys().then(data => {
      if (data.success) {
        let isChecked = {};
        for(let m in data){
          if(m === "success") continue;
          isChecked[m] = {};
          data[m].map(i =>{
            isChecked[m][i.KeyName] = true;
          });
        }
        if(window.nodeCopy){
          window.nodeCopy.metric.map(m => {
            isChecked[m.type] = m.attributes;
          });
        }
        this.setState({
          items: data,
          isChecked: isChecked,
        });
      }
    })
  }

  getChecked = (m, k) => {
    let checked = true;
    try{
      checked = this.state.isChecked[m][k];
    }catch(e){}
    if(checked === undefined){
      checked = true;
    }
    return checked;
  }

  renderFormItem = () => {
    let result = []
    let selectedMetric = config.get("metric");
    if(!selectedMetric) selectedMetric = [];
    const {getFieldDecorator} = this.props.form;
    selectedMetric.map(metric => {
      let key = metric.type;
      let ele = this.state.items[key];
      if(!ele || ele.length <= 0){
        return true;
      }
      result.push(
        <FormItem key={key}>
          <dis>
            <span><b>{key} 字段配置</b></span>
            {/*<span style={{marginLeft:"26px"}}>*/}
              {/*<Checkbox*/}
                {/*key={key}*/}
                {/*indeterminate={this.state.indeterminate[key]}*/}
                {/*onChange={this.onCheckAllChange[key]}*/}
                {/*checked={this.state.checkAll[key]}*/}
              {/*>全选/全不选</Checkbox>*/}
            {/*</span>*/}
            <hr/>
          </dis>
            <Row style={{textAlign: "left", paddingLeft: "26px"}}>
              {ele.map(i => (
                getFieldDecorator(`${key}.${i.key}`, {valuePropName: 'checked', initialValue: this.getChecked(key, i.key)})(
                  <Col span={12} key={i.key}>
                    <Checkbox defaultChecked={this.getChecked(key, i.key)}>{i.key+" ("+i.value+")"}</Checkbox>
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