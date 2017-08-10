Vue.use(TreeView)

var ve = new Vue({
	el: '#main',
	data: {
		ak: "AccessKey",
		sk: "SecretKey",
		repo: "repo",
		grokMulti: "",
		grokMultiActive: "",
		grokMultiPrimary: "",
		activeMultiGrokHeadRegexp: "none",
		sampleLog: `[05-May-2017 13:44:39]  [pool log] pid 4109
script_filename = /data/html/log.ushengsheng.com/index.php
[0x00007fec119d1720] curl_exec() /data/html/xyframework/base/XySoaClient.php:357
[0x00007fec119d1590] request_post() /data/html/xyframework/base/XySoaClient.php:284
[0x00007fff39d538b0] __call() unknown:0
[0x00007fec119d13a8] add() /data/html/log.ushengsheng.com/1/interface/ErrorLogInterface.php:70
[0x00007fec119d1298] log() /data/html/log.ushengsheng.com/1/interface/ErrorLogInterface.php:30
[0x00007fec119d1160] android() /data/html/xyframework/core/x.php:215
[0x00007fec119d0ff8] +++ dump failed
[05-May-2017 13:45:39]  [pool log] pid 4108`,
		points: "",
		grokPatterns: "%{PHP_FPM_SLOW_LOG}",
		headLinePattern: `^\\[\\d+-\\w+-\\d+\\s\\d+:\\d+:\\d+]\\s.*`,
		customPattern: `
PHPLOGTIMESTAMP (%{MONTHDAY}-%{MONTH}-%{YEAR}|%{YEAR}-%{MONTHNUM}-%{MONTHDAY}) %{HOUR}:%{MINUTE}:%{SECOND}
PHPTZ (%{WORD}\\/%{WORD})
PHPTIMESTAMP \\[%{PHPLOGTIMESTAMP:timestamp}(?:\\s+%{PHPTZ}|)\\]

PHPFPMPOOL \\[pool %{WORD:pool}\\]
PHPFPMCHILD child %{NUMBER:childid}

FPMERRORLOG \\[%{PHPLOGTIMESTAMP:timestamp}\\] %{WORD:type}: %{GREEDYDATA:message}
PHPERRORLOG %{PHPTIMESTAMP} %{WORD:type} %{GREEDYDATA:message}

PHP_FPM_SLOW_LOG (?m)^\\[%{PHPLOGTIMESTAMP:timestamp}\\]\\s\\s\\[%{WORD:type}\\s%{WORD}\\]\\s%{GREEDYDATA:message}$`,

		selected: 'raw',
		grokDisplay: "none",
		parserConfig: `"parser":{
}`,
		options: [{
				text: '原始日志（raw log)',
				value: 'raw'
			},
			{
				text: 'json日志(json log)',
				value: 'json'
			},
			{
				text: 'grok日志（grok log)',
				value: 'grok'
			}
		],

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
		selected: function(val) {
			switch(val) {
				case "raw", "json":
					this.grokDisplay = "none"
					this.activeMultiGrokHeadRegexp = "none"
					break
				case "grok":
					this.grokDisplay = ""
					break
			}
		}
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
			if(r == true) {
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

			var data
			switch(this.selected) {
				case 'raw':
					for(var i = 0; i < this.sampleLog.split("\n").length; i++) {
						log = log + "\\n" + this.sampleLog.split("\n")[i]
					}
					data = '{"sampleLog":"' + log + '","logType":"' + this.selected + '"}'
					break
				case 'json':
					data = '{"sampleLog":"' + btoa(unescape(encodeURIComponent(this.sampleLog))) + '","logType":"' + this.selected + '"}'
					break
				case 'grok':
					for(var i = 0; i < this.sampleLog.split("\n").length; i++) {
						log = log + "\\n" + this.sampleLog.split("\n")[i]
					}
					data = '{"sampleLog":"' + log + '","logType":"' + this.selected +
						'", "grok_mode":"' + this.grokMulti + '","grok_patterns":"' + btoa(this.grokPatterns) +
						'","grok_line_head_pattern":"' + btoa(this.headLinePattern) +
						'","grok_custom_patterns":"' + btoa('\n' + this.customPattern) + '"}'
					break
			}
			this.$http.post('/logkit/parse',
				data, {
					emulateJSON: true
				}).then(response => {
				// get body data
				this.points = JSON.stringify(response.body, null, 2);
				this.buildConf()
			}, response => {
				// error callback
				this.points = response.body;
			});

		},
		activeMultiGrok: function() {
			if(this.grokMultiActive == "") {
				this.grokMultiActive = "active"
				this.grokMultiPrimary = "btn-primary"
				this.activeMultiGrokHeadRegexp = ""
				this.grokMulti = "multi"

			} else {
				this.grokMultiActive = ""
				this.grokMultiPrimary = ""
				this.activeMultiGrokHeadRegexp = "none"
				this.grokMulti = ""
			}

		},
		buildConf: function() {
			var now = new Date()
			var date = now.getMinutes() + now.getUTCSeconds();
			conf = {
				"name": this.selected + "-parser-" + date,
				"type": this.selected,
				"labels": ""
			}
			switch(this.selected) {
				case "grok":
					conf["grok_mode"] = this.grokMulti
					conf["grok_patterns"] = this.grokPatterns
					conf["grok_custom_patterns"] = this.customPattern.split(String.fromCharCode(92)).join(String.fromCharCode(92))
					conf["grok_line_head_pattern"] = this.headLinePattern
					
					conf["timezone_offset"] = "+08"
					break

			}

			this.parserConfig = '"parser":' + JSON.stringify(conf, null, 2).split(String.fromCharCode(92, 92)).join(String.fromCharCode(92))
			return conf
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
			console.log(config)
			for (var prop in this.curReaderDefaultOption) {
				config[prop] = this.curReaderDefaultOption[prop]
			}
			this.readerConfig = '"reader":' + JSON.stringify(config, null, 2)
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
					//this.readerSelectOption()
					//this.readerTypeOptions=this.myNewoptions
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
						// The request was made and the server responded with a status code
						// that falls out of the range of 2xx
						console.log(error.response);
						alert("添加失败： " + error.response.data.message)
					} else if(error.request) {
						// The request was made but no response was received
						// `error.request` is an instance of XMLHttpRequest in the browser and an instance of
						// http.ClientRequest in node.js
						console.log(error.request);
					} else {
						// Something happened in setting up the request that triggered an Error
						console.log('Error', error.message);
					}
					console.log(error.config);
				});
		},
		formatRunnerConfig: function() {
			this.buildReaderConfig()
			this.buildConf() //parser config
			this.buildSenderConfig()
			this.runnerConfig = '{\n"name":"' + this.runnerName + '",\n' + this.readerConfig + ",\n" + this.parserConfig + ",\n" + this.senderConfig + "\n}"
			this.runnerConfig = JSON.stringify(JSON.parse(this.runnerConfig), null, 2)
		},
		getCurReaderOption: function(){
			this.curReaderDefaultOption = {}
			this.curReaderOption = this.readerOptions[this.readerTypeSelected]
			for (var prop in this.curReaderOption) {
				this.curReaderDefaultOption[prop] = this.curReaderOption[prop].Default
				if  (this.curReaderOption[prop].ChooseOnly==true) {
					this.curReaderDefaultOption[prop] = this.curReaderOption[prop].ChooseOptions[0]
				}
				if  (this.curReaderOption[prop].DefaultNoUse==true) {
					this.curReaderOptionStyle[prop] = "width: 350px; color:red;"
				}else{
					this.curReaderOptionStyle[prop] = "width: 350px;"
				}
			}
		},
		onReaderTypeChange: function() {
			this.getCurReaderOption()
		},
		changeTextColor: function(value){
			this.curReaderOptionStyle = Object.assign({}, this.curReaderOptionStyle, {[value]: "width: 350px;"})
		}
	},
	computed: {

	},

	mounted() {
		let that = this
		axios.get('/logkit/readerusages').then(function(response) {
			that.readerTypeOptions = response.data;
		}).catch(function(error) {
			console.log(error);
		});
		axios.get('/logkit/readeroptions').then(function(response) {
			that.readerOptions = response.data;
			that.getCurReaderOption()
		}).catch(function(error) {
			console.log(error);
		});		
	}
})
ve.getlists()
