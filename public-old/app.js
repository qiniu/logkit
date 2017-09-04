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
		parserSampleLogs: {},
		parserPoints: "",
		curSampleLog: "",
		parserPointsButtonTips: " ← 点此按钮可以调试您的配置!",

		//sender related params
		senderTypeSelected: 'pandora',
		senderTypeOptions: [],
		senderOptions: {},
		curSenderOption: {},
		curSenderDefaultOption: {},
		curSenderOptionStyle: {},
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
			reqdata['sampleLog'] = this.curSampleLog
			let that = this
			axios.post("/logkit/parser/parse", reqdata)
				.then(function(response) {
					that.parserPoints = JSON.stringify(response.data, null, 2);
					that.parserPointsButtonTips = " 解析成功！"
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
					that.parserPointsButtonTips = " 解析失败！"
				});
		},
		buildSenderConfig: function() {
			var config = {
				"name": "pandora.sender." + timenow(),
				"sender_type": this.senderTypeSelected,
			}
			for(var prop in this.curSenderDefaultOption) {
				if (this.curSenderDefaultOption[prop] === ""){
					continue;
				}
				config[prop] = this.curSenderDefaultOption[prop]
			}
			this.senderConfig = '"senders":[' + JSON.stringify(config, null, 2) + "]"
			return config
		},
		buildReaderConfig: function() {
			var config = {
				"mode": this.readerTypeSelected,
			}
			for(var prop in this.curReaderDefaultOption) {
				if (this.curReaderDefaultOption[prop]===""){
					continue;
				}
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
				if (this.curParserDefaultOption[prop] ===""){
					continue;
				}
				if (prop==="grok_custom_patterns"){
					config[prop] = btoa(this.curParserDefaultOption[prop])
				}else{
					config[prop] = this.curParserDefaultOption[prop]
				}
				console.log(prop,config[prop])
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
					this.runnerName = "logkit.runner." + timenow()
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
				var value = this.curReaderOption[prop]
				this.curReaderDefaultOption[value.KeyName] = value.Default
				if(value.ChooseOnly) {
					this.curReaderDefaultOption[value.KeyName] = value.ChooseOptions[0]
				}
				if(value.DefaultNoUse) {
					this.curReaderOptionStyle[value.KeyName] = "width: 350px; color:red;"
				} else {
					this.curReaderOptionStyle[value.KeyName] = "width: 350px;"
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
				var value = this.curParserOption[prop]
				this.curParserDefaultOption[value.KeyName] = value.Default
				if(value.ChooseOnly) {
					this.curParserDefaultOption[value.KeyName] = value.ChooseOptions[0]
				}
				if(value.KeyName === "name") {
					this.curParserDefaultOption[value.KeyName] = "pandora.parser." + timenow()
				}
				if(value.DefaultNoUse) {
					this.curParserOptionStyle[value.KeyName] = "width: 350px; color:red;"
				} else {
					this.curParserOptionStyle[value.KeyName] = "width: 350px;"
				}
			}
		},
		onParserTypeChange: function() {
			this.getCurParserOption()
			try {
				this.curSampleLog = this.parserSamplelogs[this.parserTypeSelected]
			} catch(err) {}
			this.parserPoints = ""
			this.parserPointsButtonTips = " ← 点此按钮可以调试您的配置!"
		},
		changeParserTextColor: function(value) {
			this.curParserOptionStyle = Object.assign({}, this.curParserOptionStyle, {
				[value]: "width: 350px;"
			})
		},

		getCurSenderOption: function() {
			this.curSenderDefaultOption = {}
			this.curSenderOption = this.senderOptions[this.senderTypeSelected]
			for(var prop in this.curSenderOption) {
				var value = this.curSenderOption[prop]
				this.curSenderDefaultOption[value.KeyName] = value.Default
				if(this.curSenderOption[prop].ChooseOnly) {
					this.curSenderDefaultOption[value.KeyName] = this.curSenderOption[prop].ChooseOptions[0]
				}
				if(this.curSenderOption[prop].DefaultNoUse) {
					this.curSenderOptionStyle[value.KeyName] = "width: 350px; color:red;"
				} else {
					this.curSenderOptionStyle[value.KeyName] = "width: 350px;"
				}
			}
		},
		onSenderTypeChange: function() {
			this.getCurSenderOption()
		},
		changeSenderTextColor: function(value) {
			this.curSenderOptionStyle = Object.assign({}, this.curSenderOptionStyle, {
				[this.curSenderOption[prop]]: "width: 350px;"
			})
		},
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

		//prepare sender options
		axios.get('/logkit/sender/usages').then(function(response) {
			that.senderTypeOptions = response.data;
		}).catch(function(error) {
			console.log(error);
		});
		axios.get('/logkit/sender/options').then(function(response) {
			that.senderOptions = response.data;
			that.getCurSenderOption()
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