import React, {Component} from 'react';
import {
  Form,
  Input,
  Select,
  Icon,
  notification,
  InputNumber,
  Checkbox,
  Button,
  Popover,
  Tooltip,
  Popconfirm
} from 'antd';
import {getTransformOptions, getTransformUsages} from '../services/logkit';
import config from '../store/config'
import moment from 'moment'
import _ from "lodash"
import {CopyToClipboard} from 'react-copy-to-clipboard'

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

class Transformer extends Component {
  constructor(props) {
    super(props);
    this.state = {
      current: 0,
      items: [],
      options: [],
      currentOption: '',
      currentItem: [],
      tags: [],
      transforms: {},
      transformerTypes: [],
      advanceChecked: false,
      isEdit: false,
      editTag: '',
      isViewTag: []
    }

    this.schemaUUID = 0;
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
    config.set('transforms', _.values(this.state.transforms))
  }


  init = () => {
    const {getFieldDecorator, setFieldsValue, resetFields} = this.props.form;
    getTransformOptions().then(item => {
      if (item.code === 'L200') {
        let options = item.data
        this.setState({
          options: options,
          currentOption: '请选择需要转化的类型',
          items: item.data,
          currentItem: []
        })

        if (window.nodeCopy && window.nodeCopy.transforms) {
          let data = {}
          let _key = []
          let transforms = {}
          let initValue = ''
          data.spec = _.reduce(
              _.map(window.nodeCopy.transforms),
              (result, item) => {
                if (item.key) {
                  initValue = `(${item.key})`
                }
                const cItem = {...item, type: `${item.type}${initValue}`}
                result["uuid" + this.schemaUUID] = cItem;
                _key.push("uuid" + this.schemaUUID);
                getFieldDecorator(`spec.${"uuid" + this.schemaUUID}.key`, {initialValue: item.key});
                getFieldDecorator(`spec.${"uuid" + this.schemaUUID}.type`, {initialValue: `${item.type}${initValue}`});
                getFieldDecorator(`spec.${"uuid" + this.schemaUUID}.stage`, {initialValue: item.stage});
                _.set(transforms, "uuid" + this.schemaUUID, item);
                this.schemaUUID++;
                return result
              },
              {});
          resetFields();
          setFieldsValue(data);

          this.setState({
            transforms,
            tags: _key
          })
        }
      }

    })

    getTransformUsages().then(item => {
      if (item.code === 'L200') {
        this.setState({
          transformerTypes: item.data
        })
      }
    })


  }
  
  copyTagConfig = (index) => {
    const isCopied = []
    isCopied[index] = true
    this.setState({
      isCopied
    })
  }
  
  onVisibleChange = (index) => {
    const isCopied = []
    isCopied[index] = false
    this.setState({
      isCopied
    })
  }

  renderTags = () => {
    const {getFieldDecorator, getFieldValue} = this.props.form;

    return this.state.tags.map((k, index) => {
      const tag = this.state.transforms && this.state.transforms[k]
        return (
        <div key={`spec.fields.${k}`} style={{ position: "relative"}} className="transformer-tag-container">
            <FormItem
                style={{ textAlign: 'left' }}>
              {getFieldDecorator(`spec.${k}.type`, {
                rules: [{required: true, message: '字段不能为空'},
                  {min: 1, max: 100, message: '长度在 1 到 100 个字符'}]
              })(<Input disabled={true} className="transformer-tag"/>)}
            </FormItem>
            {
              this.state.isReadonly ? null :
                  <span>
                    <Icon
                      style={{marginTop: "23px"}}
                      className="dynamic-delete-button"
                      type="edit"
                      onClick={() => this.editTag(k)}
                      title="编辑"
                    />
                    <Popover
                      trigger="click"
                      content={
                        JSON.stringify(tag, null, '\t')} overlayStyle={{whiteSpace: 'pre'}}>
                      <Icon
                        style={{marginTop: "23px"}}
                        className="dynamic-delete-button"
                        type="eye-o"
                        title="查看"
                      />
                    </Popover>
                    <CopyToClipboard onCopy={() => this.copyTagConfig(index)} text={JSON.stringify(tag, null, '\t')}>
                      <Tooltip visible={this.state.isCopied && this.state.isCopied[index]} title="复制成功" onVisibleChange={() => this.onVisibleChange(index)}>
                        <Icon
                          style={{marginTop: "23px"}}
                          className="dynamic-delete-button"
                          type="copy"
                          title="复制"
                        />
                      </Tooltip>
                    </CopyToClipboard>
                    <Popconfirm title={"是否删除该转换器?"} onConfirm={() => this.removeTag(k)}>
                      <Icon
                        style={{marginTop: "23px"}}
                        className="dynamic-delete-button"
                        type="delete"
                        title="删除"
                      />
                    </Popconfirm>
                  </span>
            }
          </div>
      )
    })
  };

  renderFormItem = () => {
    const { getFieldDecorator, getFieldValue } = this.props.form;
    const currentItem = this.state.currentItem || []
    let result = []
    let advancedResults = []
    currentItem.map((ele, index) => {
      let formItem = null
      const labelDes = (
        ele.Description.indexOf('(') !== -1 ? <span>
          {ele.Description.slice(0, ele.Description.indexOf('('))}
          <br />
          <span style={{ color: 'rgba(0,0,0,.43)', float: 'right' }}>
            {ele.Description.slice(ele.Description.indexOf('('), ele.Description.length)}
          </span>
        </span>
          : <span>{ele.Description}</span>
      )
      let advanceDependValue = ele.advance_depend && getFieldValue(`${this.state.currentOption}.${ele.advance_depend}`)
      let isAdvanceDependHide = advanceDependValue === 'false' || advanceDependValue === false
      if (ele.ChooseOnly == false) {
        if (ele.KeyName == 'name') {
          ele.Default = "pandora.sender." + moment().format("YYYYMMDDHHmmss");
        }
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
            })(ele.Type === 'string' ? (<Input placeholder={ele.DefaultNoUse ? ele.placeholder : '空值可作为默认值'} disabled={this.state.isReadonly} />) :
              (<InputNumber placeholder={ele.DefaultNoUse ? ele.placeholder : '空值可作为默认值'} disabled={this.state.isReadonly} />)
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
              initialValue: ele.Default || ele.ChooseOptions[0]
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
      currentItem: option != '请选择需要转化的类型' ? this.state.items[option] : []
    })

  }

  renderSelectOptions = () => {
    let options = []
    options.push(<Option key={'请选择需要转化的类型'} value={'请选择需要转化的类型'}>{'请选择需要转化的类型(若无,直接到下一步)'}</Option>)
    this.state.transformerTypes.map((ele) => {
      options.push(<Option key={ele.key} value={ele.key}>{ele.key + "  （" + ele.value + "）"}</Option>)
    })
    return (
        options
    )
  }

  renderChooseOption = (items) => {
    let options = []
    items.map((ele) => {
        let el = ele
        if (typeof el === 'boolean'){
           el = String(el)
        }

        options.push(<Option key={ele} value={ele}>{el}</Option>)
    })
    return (
        options
    )
  }

  addTag = () => {
    const {getFieldsValue, getFieldDecorator, setFieldsValue} = this.props.form;
    let data = getFieldsValue();
    let initValue = ''
    if (this.state.currentOption === 'script') {
      if (data && data[this.state.currentOption] && data[this.state.currentOption]['script']) {
        data[this.state.currentOption]['script'] = window.btoa(encodeURIComponent(data[this.state.currentOption]['script']))
      }
    }
    if (data[this.state.currentOption].key) {
      initValue = `(${data[this.state.currentOption].key})`
    }
    if (!this.state.isEdit) {
      if (this.state.currentOption != '请选择需要转化的类型') {
        this.setState({
          tags: this.state.tags.concat(`uuid${this.schemaUUID}`)
        });
        // getFieldDecorator(`spec.${"uuid" + this.schemaUUID}.key`, {
        //   initialValue: data[this.state.currentOption].key,
        //   rules: [{required: true, message: '源字段不能为空'},
        //     {min: 1, max: 100, message: '长度在 1 到 100 个字符'}]
        // });
        getFieldDecorator(`spec.${"uuid" + this.schemaUUID}.type`, {
          initialValue: `${data[this.state.currentOption].type}${initValue}`,
          rules: [{required: true, message: '源字段不能为空'},
            {min: 1, max: 100, message: '长度在 1 到 100 个字符'}]
        });
    
        let transforms = this.state.transforms
        let key = "uuid" + this.schemaUUID
        _.set(transforms, key, data[this.state.currentOption]);
        this.setState({
          transforms
        }, () => {
          this.handleChange('请选择需要转化的类型')
        })
    
        this.schemaUUID++;
      } else {
        notification.warning({message: "未选择具体类型", description: '请选择需要转化的类型再添加', duration: 10})
      }
    } else {
      setFieldsValue({
        [`spec.${this.state.editTag}.type`]: `${data[this.state.currentOption].type}${initValue}`
      })
      let transforms = this.state.transforms
      let key = this.state.editTag
      _.set(transforms, key, data[this.state.currentOption]);
      this.setState({
        transforms,
        isEdit: false
      }, () => {
        this.handleChange('请选择需要转化的类型')
      })
    }


  };
  
  editTag = (k) => {
    this.setState({
      isEdit: true,
      editTag: k
    })
    const {setFieldsValue} = this.props.form
    const tag = this.state.transforms[k]
    this.handleChange(tag && tag.type)
    const data = {}
    const type = tag.type
    for (const key in tag) {
      if (key !== 'type') {
        data[`${type}.${key}`] = tag[key]
      }
    }
    setTimeout(() => {setFieldsValue(data)}, 0)
  }

  removeTag = (k) => {
    this.setState({
      tags: this.state.tags.filter(key => key !== k),
      transforms: _.omit(this.state.transforms, k),
      isEdit: false
    }, () => {
      this.handleChange('请选择需要转化的类型')
    })
  };

  render() {
    const {getFieldDecorator} = this.props.form
    const renderResults = this.renderFormItem()
    return (
        <div >
          <Form className="slide-in text-color">
            <FormItem {...optionFormItemLayout} label="需要转化字段的类型">
              {getFieldDecorator(`${this.state.currentOption}.type`, {
                initialValue: this.state.currentOption
              })(
                  <Select onChange={this.handleChange}>
                    {this.renderSelectOptions()}
                  </Select>)}
            </FormItem>
            <div className="ant-divider ant-divider-horizontal"></div>
            {renderResults.result}
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
            <div className="option-add">
              <FormItem {...optionFormItemLayout} label={<span style={{display:'none'}}></span>}>
                <div className="option-add-btn">
                  <Button onClick={this.addTag} type="primary" className="option-add-tag-btn">
                    {this.state.isEdit ? '确认' : '添加'}
                  </Button>
                </div>
              </FormItem>
            </div>
          <div className={this.state.tags.length>0 ? 'render-tag-container' : ''}>{this.renderTags()}</div>
          </Form>
        </div>
    );
  }
}
export default Form.create()(Transformer);