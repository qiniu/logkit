import React, {Component} from 'react';
import {notification, Button, Steps, Icon, Tag, Layout} from 'antd';
import Source from  '../components/sourceConfig'
import Parser from  '../components/parserConfig'
import Sender from '../components/senderConfig'
import RenderConfig from '../components/renderConfig'
import Transformer from '../components/transformer'
import config from '../store/config'
import {isJSON} from '../utils/tools'
import moment from 'moment'
import {
	postConfigData,
	putConfigData,
	postClusterConfigData,
	putClusterConfigData
} from '../services/logkit';
import _ from "lodash";

const Step = Steps.Step;
const {Header, Content, Footer, Sider} = Layout;
const steps = [{
	title: '配置数据源',
	content: '配置相关数据源信息',
}, {
	title: '配置解析方式',
	content: '配置相关解析方式',
}, {
	title: '配置Transformer(非必填)',
	content: '配置相关字段解析',
}, {
	title: '配置发送方式',
	content: '配置相关发送方式',
}, {
	title: '确认并添加收集器',
	content: '确认并添加收集器',
}];
class CreateLogRunner extends Component {
	constructor(props) {
		super(props);
		this.state = {
			current: 0,
			isCpoyStatus: false,
			sourceConfigCheck: false,
			version: ''
		};
		window.clearInterval(window.statusInterval);
	}

	componentDidMount() {
		this.init()
	}


	componentWillUnmount() {

	}

	componentDidUpdate(prevProps) {

	}

	init = () => {
		let that = this
		if (window.isCopy === true) {
			this.setState({
				isCpoyStatus: true
			})
		}
		if (window.nodeCopy) {
			config.delete("metric");
		}
	}

	next() {

		let that = this;
		this.setState({
			sourceConfigCheck: true
		})
		if (this.state.current === 0) {
			that.refs.checkSourceData.validateFields(null, {}, (err) => {
				if (err) {
					notification.warning({message: "表单校验未通过,请检查", duration: 20,})
				} else {
					const current = this.state.current + 1;
					this.setState({current});
				}
			});
		} else if (this.state.current === 1) {
			that.refs.checkParseData.validateFields(null, {}, (err) => {
				if (err) {
					notification.warning({message: "表单校验未通过,请检查", duration: 20,})
				} else {
					const current = this.state.current + 1;
					this.setState({current});
				}
			});
		} else if (this.state.current === 2) {

			let formValue = this.refs.initTransform.getFieldsValue()

			if(formValue['请选择需要转化的类型'] == undefined){
				notification.warning({message: "您当前选择的transform未添加", description: '请点击添加按钮添加', duration: 10})
				return
			}

			const current = this.state.current + 1;
			this.setState({current});

		} else if (this.state.current === 3) {
			that.refs.checkSenderData.validateFields(null, {}, (err) => {
				if (err) {
					notification.warning({message: "表单校验未通过,请检查", duration: 20,})
				} else {
					const current = this.state.current + 1;
					this.setState({current});
					let name = "runner." + moment().format("YYYYMMDDHHmmss");
					let batch_interval = that.refs.initConfig.getFieldValue('batch_interval')
					let collect_interval = that.refs.initConfig.getFieldValue('collect_interval')
					let runnerName = that.refs.initConfig.getFieldValue('name')
					if (window.isCopy && window.nodeCopy) {
						name = window.nodeCopy.name
					}
					let nodeData = config.getNodeData()
					if (nodeData && nodeData.parser.type === 'grok') {
						if (nodeData.parser.grok_custom_patterns != '' && nodeData.parser.grok_custom_patterns != undefined) {
							nodeData.parser.grok_custom_patterns = window.btoa(nodeData.parser.grok_custom_patterns)
						}

					}

					if (window.isCopy && window.nodeCopy) {
						runnerName = window.nodeCopy.name
						batch_interval = window.nodeCopy.batch_interval
						collect_interval = window.nodeCopy.collect_interval
					}
					let data = {
						name: runnerName != undefined ? runnerName : name,
						batch_interval: batch_interval != undefined ? batch_interval : 60,
						collect_interval: collect_interval != undefined ? collect_interval : 3,
						...config.getNodeData()
					}
					that.refs.initConfig.setFieldsValue({config: JSON.stringify(data, null, 2)});
					that.refs.initConfig.setFieldsValue({name: runnerName != undefined ? runnerName : name});
					that.refs.initConfig.setFieldsValue({batch_interval: batch_interval != undefined ? batch_interval : 60});
					that.refs.initConfig.setFieldsValue({collect_interval: collect_interval != undefined ? collect_interval : 3});
				}
			});
		}

	}

	addRunner = () => {
		const { currentTagName, currentMachineUrl } = this.props
		const {handleTurnToRunner} = this.props
		let that = this
		const {validateFields, getFieldsValue} =  that.refs.initConfig;
		let formData = getFieldsValue();
		validateFields(null, {}, (err) => {
			if (err) {
				notification.warning({message: "表单校验未通过,请检查", duration: 20,})
				return
			} else {
				if (isJSON(formData.config)) {
					let data = JSON.parse(formData.config);
					let tag = (currentTagName != null && currentTagName != undefined) ? currentTagName : ''
					let url = (currentMachineUrl != null && currentMachineUrl != undefined) ? currentMachineUrl : ''
					if (window.isCluster && window.isCluster === true) {
						postClusterConfigData({name: data.name, tag: tag, url: url, body: data}).then(data => {
							if (data && data.code === 'L200') {
								notification.success({message: "收集器添加成功", duration: 10,})
								handleTurnToRunner()
							}

						})
					} else {
						postConfigData({name: data.name, body: data}).then(data => {
							if (data && data.code === 'L200') {
								notification.success({message: "收集器添加成功", duration: 10,})
								handleTurnToRunner()
							}

						})
					}
				} else {
					notification.warning({message: "不是一个合法的json对象,请检查", duration: 20,})
				}
			}
		});

	}

	updateRunner = () => {
		const currentTagName = window.nodeCopy.tag
		const currentMachineUrl = window.nodeCopy.machineUrl
		// const { currentTagName, currentMachineUrl } = this.props
		const {handleTurnToRunner} = this.props
		let that = this
		const {validateFields, getFieldsValue} =  that.refs.initConfig;
		let formData = getFieldsValue();
		validateFields(null, {}, (err) => {
			if (err) {
				notification.warning({message: "表单校验未通过,请检查", duration: 20,})
				return
			} else {
				if (isJSON(formData.config)) {
					let data = JSON.parse(formData.config);
					let tag = (currentTagName != null && currentTagName != undefined) ? currentTagName : ''
					let url = (currentMachineUrl != null && currentMachineUrl != undefined) ? currentMachineUrl : ''
					if (window.isCluster && window.isCluster === true) {
						putClusterConfigData({name: data.name, tag: tag, url: url, body: data}).then(data => {
							if (data && data.code === 'L200') {
								notification.success({message: "收集器修改成功", duration: 10,})
								handleTurnToRunner()
							}

						})
					} else {
						putConfigData({name: data.name, body: data}).then(data => {
							if (data && data.code === 'L200') {
								notification.success({message: "收集器修改成功", duration: 10,})
								handleTurnToRunner()
							}
						})
					}
				} else {
					notification.warning({message: "不是一个合法的json对象,请检查", duration: 20,})
				}
			}
		});

	}

	prev() {
		const current = this.state.current - 1;
		this.setState({current});
	}

	turnToIndex() {
		window.nodeCopy = config.getNodeData()
		this.props.router.push({pathname: `/`})
	}

	render() {
		const {current} = this.state;
		return (
			<div className="logkit-create-container">
				<Steps current={current}>
					{steps.map(item => <Step key={item.title} title={item.title}/>)}
				</Steps>
				<div className="steps-content">
					<div><p className={this.state.current <= 3 ? 'show-div info' : 'hide-div'}>注意：黄色字体选框需根据实际情况修改，其他可作为默认值</p>
					</div>
					<div className={this.state.current === 0 ? 'show-div' : 'hide-div'}>
						<Source ref="checkSourceData"></Source>
					</div>
					<div className={this.state.current === 1 ? 'show-div' : 'hide-div'}>
						<Parser ref="checkParseData"></Parser>
					</div>
					<div className={this.state.current === 2 ? 'show-div' : 'hide-div'}>
						<Transformer ref="initTransform"></Transformer>
					</div>
					<div className={this.state.current === 3 ? 'show-div' : 'hide-div'}>
						<Sender ref="checkSenderData"></Sender>
					</div>
					<div className={this.state.current === 4 ? 'show-div' : 'hide-div'}>
						<RenderConfig ref="initConfig"></RenderConfig>
					</div>

				</div>
				<div className="steps-action">
					{
						this.state.current < steps.length - 1
						&&
						<Button type="primary" onClick={() => this.next()}>下一步</Button>
					}
					{
						this.state.current === steps.length - 1 && this.state.isCpoyStatus === false
						&&
						<Button type="primary" onClick={() => this.addRunner()}>确认并提交</Button>
					}
					{
						this.state.current === steps.length - 1 && this.state.isCpoyStatus === true
						&&
						<Button type="primary" onClick={() => this.updateRunner()}>修改并提交</Button>
					}
					{
						this.state.current > 0
						&&
						<Button style={{marginLeft: 8}} onClick={() => this.prev()}>
							上一步
						</Button>
					}
				</div>
			</div>
		);
	}
}
export default CreateLogRunner;