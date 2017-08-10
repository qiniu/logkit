Vue.use(TreeView)

function timenow() {
	var now = new Date(),
		ampm = 'am',
		mm = now.getMonth()
	if(mm < 10) mm = '0' + mm
	dd = now.getDate()
	if(dd < 10) dd = '0' + dd
	h = now.getHours(),
		m = now.getMinutes(),
		s = now.getSeconds();
	if(m < 10) m = '0' + m;
	if(s < 10) s = '0' + s;
	return '' + now.getFullYear() + mm + dd + h + m + s;
}

var ve = new Vue({
	el: '#main',
	data: {
		ak: "AccessKey",
		sk: "SecretKey",
		repo: "repo",		

		//nav related params
		readerDisplay: "none",
		parserDisplay: "none",
		senderDisplay: "none",
		configDisplay: "none",
		helpDisplay: "none",
		listDisplay: "",

		readertabactive: "",
		parsertabactive: "",
		sendertabactive: "",
		configtabactive: "",
		helptabactive: "",
		configlistactive: "active",

		//reader related params
		readerTypeSelected: 'dir',
		readerTypeOptions: [],
		readerOptions: {},
		curReaderOption: {},
		curReaderDefaultOption: {},
		curReaderOptionStyle: {},
		readerConfig: `"reader":{
  }`,
  		

		//parser related params
		parserTypeSelected: 'json',
		parserTypeOptions: [],
		parserOptions: {},
		curParserOption: {},
		curParserDefaultOption: {},
		curParserOptionStyle: {},
		parserConfig: `"parser":{
}`,
		parserSampleLogs:{},
		parserPoints: "",
		curSampleLog:"",
		parserPointsButtonTips:" ← 点此按钮可以调试您的配置!",
		
		//sender related params
		senderTypeSelected: 'pandora',
		senderTypeOptions: [{
			text: '发送数据到Pandora(Pandora Sender)',
			value: "pandora"
		}],

		pandora_gzip: "false",
		pandora_gzip_options: [{
				text: '不压缩(false)',
				value: "false"
			},
			{
				text: '压缩(true)',
				value: "true"
			},
		],
		pandora_ak: "",
		pandora_sk: "",
		pandora_repo_name: "",
		pandora_host: "https://pipeline.qiniu.com",
		pandora_region: "nb",
		request_rate_limit: "",
		flow_rate_limit: "",
		senderConfig: `"senders":[{
    }]`,

		//logkit config related
		runnerConfig: ``,
		runnerName: "",
		name: "",
		batch_size: "",
		batch_len: "",
		batch_interval: "",

		runners: []
	},
	watch: {
		
	},
	methods: {
		getlists: function() {
			axios.get('/logkit/configs')
				.then(function(response) {
					this.runners = response.data
				}.bind(this))
				.catch(function(error) {
					console.log(error);
				});
		},
		deleteTask: function(name) {
			var r = confirm("确认删除 runner: " + name + " 吗？")
			if(r) {
				axios.delete('/logkit/configs/' + name)
					.catch(function(error) {
						console.log(error);
					});
				this.getlists()
				alert("runner: " + name + " 删除成功")
			}
		},
		parse: function() {
			var now = new Date()
			var date = now.getUTCSeconds();
			var log = "";
			var reqdata = this.buildParserConf()
			reqdata['sampleLog']=this.curSampleLog
			let that = this
			axios.post("/logkit/parser/parse", reqdata)
				.then(function(response) {
					that.parserPoints = JSON.stringify(response.data, null, 2);
					that.parserPointsButtonTips=" 解析成功！"
				})
				.catch(function(error) {
					if(error.response) {
						console.log(error.response);
						that.parserPoints = error.response.data.message;
					} else if(error.request) {
						console.log(error.request);
					} else {
						console.log('Error', error.message);
					}
					console.log(error.config);
					that.parserPointsButtonTips=" 解析失败！"
				});
		},
		buildSenderConfig: function() {
			var now = new Date()
			var date = now.getMinutes() + now.getUTCSeconds();
			config = {
				"name": "pandora.sender." + date,
				"sender_type": this.senderTypeSelected,
				"pandora_ak": this.pandora_ak,
				"pandora_sk": this.pandora_sk,
				"pandora_host": this.pandora_host,
				"pandora_repo_name": this.pandora_repo_name,
				"pandora_region": this.pandora_region,
				"pandora_gzip": this.pandora_gzip,
				"pandora_schema_free": "true"
			}
			if(this.flow_rate_limit != "") {
				config["flow_rate_limit"] = this.flow_rate_limit
			}
			this.senderConfig = '"senders":[' + JSON.stringify(config, null, 2) + "]"
			return config
		},
		buildReaderConfig: function() {
			var config = {
				"mode": this.readerTypeSelected,
			}
			for(var prop in this.curReaderDefaultOption) {
				config[prop] = this.curReaderDefaultOption[prop]
			}
			this.readerConfig = '"reader":' + JSON.stringify(config, null, 2)
			return config
		},
		buildParserConf: function() {
			var config = {
				"type": this.parserTypeSelected,
			}
			for(var prop in this.curParserDefaultOption) {
				config[prop] = this.curParserDefaultOption[prop]
			}
			this.parserConfig = '"parser":' + JSON.stringify(config, null, 2).split(String.fromCharCode(92, 92)).join(String.fromCharCode(92))
			return config
		},
		resetNav: function() {
			this.readertabactive = ""
			this.parsertabactive = ""
			this.sendertabactive = ""
			this.configtabactive = ""
			this.helptabactive = ""
			this.configlistactive = ""
		},
		resetView: function() {
			this.parserDisplay = "none"
			this.readerDisplay = "none"
			this.senderDisplay = "none"
			this.configDisplay = "none"
			this.helpDisplay = "none"
			this.listDisplay = "none"
		},
		nav: function(val) {
			this.resetNav()
			this.resetView()
			switch(val) {
				case "reader":
					this.readerDisplay = ""
					this.readertabactive = "active"
					break
				case "parser":
					this.parserDisplay = ""
					this.parsertabactive = "active"
					break
				case "sender":
					this.senderDisplay = ""
					this.sendertabactive = "active"
					break
				case "config":
					this.configDisplay = ""
					this.configtabactive = "active"
					var now = new Date()
					var date = now.getMinutes() + now.getUTCSeconds();
					this.runnerName = "logkit.runner." + date
					break
				case "help":
					this.helpDisplay = ""
					this.helptabactive = "active"
					break
				case "list":
					this.getlists()
					this.listDisplay = ""
					this.configlistactive = "active"
					break
			}
		},
		addRunner: function() {
			try {
				runnerObj = JSON.parse(this.runnerConfig)
				this.runnerConfig = JSON.stringify(runnerObj, null, 2)
			} catch(err) {
				console.log(err)
			}
			axios.post("/logkit/configs/" + runnerObj.name, runnerObj)
				.then(function(response) {
					alert(runnerObj.name + " 添加成功！")
				})
				.catch(function(error) {
					if(error.response) {
						console.log(error.response);
						alert("添加失败： " + error.response.data.message)
					} else if(error.request) {
						console.log(error.request);
					} else {
						console.log('Error', error.message);
					}
					console.log(error.config);
				});
		},
		formatRunnerConfig: function() {
			this.buildReaderConfig()
			this.buildParserConf() //parser config
			this.buildSenderConfig()
			this.runnerConfig = '{\n"name":"' + this.runnerName + '",\n' + this.readerConfig + ",\n" + this.parserConfig + ",\n" + this.senderConfig + "\n}"
			this.runnerConfig = JSON.stringify(JSON.parse(this.runnerConfig), null, 2)
		},

		getCurReaderOption: function() {
			this.curReaderDefaultOption = {}
			this.curReaderOption = this.readerOptions[this.readerTypeSelected]
			for(var prop in this.curReaderOption) {
				this.curReaderDefaultOption[prop] = this.curReaderOption[prop].Default
				if(this.curReaderOption[prop].ChooseOnly) {
					this.curReaderDefaultOption[prop] = this.curReaderOption[prop].ChooseOptions[0]
				}
				if(this.curReaderOption[prop].DefaultNoUse) {
					this.curReaderOptionStyle[prop] = "width: 350px; color:red;"
				} else {
					this.curReaderOptionStyle[prop] = "width: 350px;"
				}
			}
		},
		onReaderTypeChange: function() {
			this.getCurReaderOption()
		},
		changeReaderTextColor: function(value) {
			this.curReaderOptionStyle = Object.assign({}, this.curReaderOptionStyle, {
				[value]: "width: 350px;"
			})
		},

		getCurParserOption: function() {
			this.curParserDefaultOption = {}
			this.curParserOption = this.parserOptions[this.parserTypeSelected]
			for(var prop in this.curParserOption) {
				this.curParserDefaultOption[prop] = this.curParserOption[prop].Default
				if(this.curParserOption[prop].ChooseOnly) {
					this.curParserDefaultOption[prop] = this.curParserOption[prop].ChooseOptions[0]
				}
				if(prop === "name") {
					this.curParserDefaultOption[prop] = "parser." + timenow()
				}
				if(this.curParserOption[prop].DefaultNoUse) {
					this.curParserOptionStyle[prop] = "width: 350px; color:red;"
				} else {
					this.curParserOptionStyle[prop] = "width: 350px;"
				}
			}
		},
		onParserTypeChange: function() {
			this.getCurParserOption()
			try{this.curSampleLog = this.parserSamplelogs[this.parserTypeSelected]}catch(err){}
			this.parserPoints = ""
			this.parserPointsButtonTips=" ← 点此按钮可以调试您的配置!"
		},
		changeParserTextColor: function(value) {
			this.curParserOptionStyle = Object.assign({}, this.curParserOptionStyle, {
				[value]: "width: 350px;"
			})
		}
	},
	computed: {},

	mounted() {
		this.getlists()
		let that = this
		//prepare reader options
		axios.get('/logkit/reader/usages').then(function(response) {
			that.readerTypeOptions = response.data;
		}).catch(function(error) {
			console.log(error);
		});
		axios.get('/logkit/reader/options').then(function(response) {
			that.readerOptions = response.data;
			that.getCurReaderOption()
		}).catch(function(error) {
			console.log(error);
		});

		//prepare parser options
		axios.get('/logkit/parser/usages').then(function(response) {
			that.parserTypeOptions = response.data;
		}).catch(function(error) {
			console.log(error);
		});
		axios.get('/logkit/parser/samplelogs').then(function(response) {
			that.parserSamplelogs = response.data;
			that.curSampleLog = that.parserSamplelogs[that.parserTypeSelected]
		}).catch(function(error) {
			console.log(error);
		});
		axios.get('/logkit/parser/options').then(function(response) {
			that.parserOptions = response.data;
			that.getCurParserOption()
		}).catch(function(error) {
			console.log(error);
		});
	}
})