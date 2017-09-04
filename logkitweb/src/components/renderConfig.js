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
      current: 0
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


  render() {
    const {getFieldDecorator} = this.props.form;
    return (
        <div >
          <div className='logkit-header'>
            <Row >
              <Col span={12}>
                <h2 className="logkit-title">logkit配置文件</h2>
              </Col>
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