import React, {Component} from 'react';
import {
  Form,
  Input,
  notification,
  Button,
  Row,
  Col
} from 'antd';
import config from '../store/config'
import moment from 'moment'
import {postConfigData} from '../services/logkit';

const FormItem = Form.Item;

const optionFormItemLayout = {
  labelCol: {
    xs: {span: 24},
    sm: {span: 0},
  },
  wrapperCol: {
    xs: {span: 24},
    sm: {span: 24},
  },
};

class renderConfig extends Component {
  constructor(props) {
    super(props);
    this.state = {
      current: 0,
      currentConfig: ''
    };
  }

  componentDidMount() {

  }

  componentWillUnmount() {
  }

  componentDidUpdate(prevProps) {

  }


  renderConfigFile = () => {
    const {getFieldDecorator, resetFields} = this.props.form;
    let name = "logkit.runner." + moment().format("YYYYMMDDHHmmss");
    let data = {
      name,
      ...config.getNodeData()
    }
    this.setState({
      currentConfig: JSON.stringify(data, null, 2)
    })
    getFieldDecorator("config", {initialValue: JSON.stringify(data, null, 2)});
    let formData = {}
    formData.config = this.state.currentConfig
    resetFields();
  }

  isJSON = (str) => {
    if (typeof str == 'string') {
      try {
        JSON.parse(str);
        return true;
      } catch (e) {
        return false;
      }
    }
  }

  addRunner = () => {
    const {validateFields, getFieldsValue} = this.props.form;
    let formData = getFieldsValue();
    validateFields(null, {}, (err) => {
      if (err) {
        notification.warning({message: "表单校验未通过,请检查", duration: 20,})
        return
      } else {
        if (this.isJSON(formData.config)) {
          let data = JSON.parse(formData.config);
          postConfigData({name: data.name, body: data}).then(data => {
            if (data == undefined) {
              notification.success({message: "Runner添加成功", duration: 10,})
            }

          })
        } else {
          notification.warning({message: "不是一个合法的json对象,请检查", duration: 20,})
        }
      }
    });

  }

  render() {
    const {getFieldDecorator} = this.props.form;
    return (
        <div >
          <div className='logkit-header'>
            <Row >
              <Col span={12}>
                <h2 className="logkit-title">logkit配置文件</h2>
                <Button className="btn-config" onClick={this.renderConfigFile}>生成配置文件</Button>
                <Button onClick={this.addRunner}>添加Runner</Button></Col>
              <Col span={12}>
                <h4 className="logkit-tip">← 点击生成配置文件, 可对配置文本手动修改后再添加Runner!</h4>
              </Col>
            </Row>
          </div>
          <div className='logkit-body'>
            <Row>
              <Form>
                <FormItem
                    {...optionFormItemLayout}
                >
                  {getFieldDecorator('config', {
                    initialValue: this.state.currentConfig,
                    rules: [{required: true, message: '配置文件不能为空', trigger: 'blur'}]
                  })(
                      <Input type="textarea" rows="50"/>
                  )}
                </FormItem>
              </Form>
            </Row>
          </div>
        </div>
    );
  }
}
export default Form.create()(renderConfig);