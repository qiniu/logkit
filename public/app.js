Vue.use(TreeView)
var ve = new Vue({
    el: '#main',
    data: {
        ak:"AccessKey",
        sk: "SecretKey",
        repo: "repo",
        grokMulti:"",
        grokMultiActive:"",
        grokMultiPrimary:"",
        activeMultiGrokHeadRegexp:"none",
        sampleLog:`[05-May-2017 13:44:39]  [pool log] pid 4109
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
        headLinePattern:`^\\[\\d+-\\w+-\\d+\\s\\d+:\\d+:\\d+]\\s.*`,
        customPattern:`
PHPLOGTIMESTAMP (%{MONTHDAY}-%{MONTH}-%{YEAR}|%{YEAR}-%{MONTHNUM}-%{MONTHDAY}) %{HOUR}:%{MINUTE}:%{SECOND}
PHPTZ (%{WORD}\\/%{WORD})
PHPTIMESTAMP \\[%{PHPLOGTIMESTAMP:timestamp}(?:\\s+%{PHPTZ}|)\\]

PHPFPMPOOL \\[pool %{WORD:pool}\\]
PHPFPMCHILD child %{NUMBER:childid}

FPMERRORLOG \\[%{PHPLOGTIMESTAMP:timestamp}\\] %{WORD:type}: %{GREEDYDATA:message}
PHPERRORLOG %{PHPTIMESTAMP} %{WORD:type} %{GREEDYDATA:message}

PHP_FPM_SLOW_LOG (?m)^\\[%{PHPLOGTIMESTAMP:timestamp}\\]\\s\\s\\[%{WORD:type}\\s%{WORD}\\]\\s%{GREEDYDATA:message}$`,

        selected: 'raw',
        grokDisplay:"none",
        parserConfig:`"parser":{
}`,
        options: [
            { text: '原始日志（raw log)', value: 'raw' },
            { text: 'json日志(json log)', value: 'json' },
            { text: 'grok日志（grok log)', value: 'grok' }
        ],
        
        //nav related params
        readerDisplay:"none",
        parserDisplay:"none",
        senderDisplay:"none",
        configDisplay:"none",
        helpDisplay:"none",
        listDisplay:"",

        readertabactive:"",
        parsertabactive:"",
        sendertabactive:"",
        configtabactive:"",
        helptabactive:"",
        configlistactive:"active",

        //reader related params
        readerTypeSelected:'fileReader',
        readerTypeOptions:[
            { text:'文件类型(file reader)', value:"fileReader",help:"读取文件中的数据"}
        ],
        readFromTypeSelected:'oldest',
        readFromTypeOptions:[
            { text:'从最新的读取', value:"newest"},
            { text:'从最老的读取', value:"oldest"}
        ],
        modeSelected:"dir",
        modeOptions:[
            { text:"文件夹模式", value:"dir"},
            { text:"单文件模式", value:"file"},
            { text:"tailx模式", value:"tailx"}
        ],
        log_path:"/home/users/john/*/auditLog",
        meta_path:"",
        encoding:"utf-8",
        valid_file_pattern:"*",
        readerConfig:`"reader":{
  }`,

        //sender related params
        senderTypeSelected:'pandora',
        senderTypeOptions:[
            { text:'发送数据到Pandora(Pandora Sender)', value:"pandora"}
        ],

        pandora_gzip:"false",
        pandora_gzip_options:[
            { text:'不压缩(false)', value:"false"},
            { text:'压缩(true)', value:"true"},
        ],
        pandora_ak:"",
        pandora_sk:"",
        pandora_repo_name:"",
        pandora_host:"https://pipeline.qiniu.com",
        pandora_region:"nb",
        request_rate_limit:"",
        flow_rate_limit:"",
        senderConfig:`"senders":[{
    }]`,

        //logkit config related
        runnerConfig:``,
        runnerName:"",
        name:"",
        batch_size:"",
        batch_len:"",
        batch_interval:"",

        runners:[]
    },
    watch: {
       selected: function(val){
           switch (val){
               case "raw","json":
                    this.grokDisplay = "none"
                    this.activeMultiGrokHeadRegexp = "none"
                    break
                case "grok":
                    this.grokDisplay = ""
                    break 
           }
       }
    },
    methods:{
        getlists: function(){
            axios.get('/logkit/configs')
                .then(function (response) {
                    this.runners=response.data
                    console.log(this.runners);
                }.bind(this))
                .catch(function (error) {
                    console.log(error);
                });
        },
        deleteTask: function(name){
            console.log(name)
            axios.delete('/logkit/configs/'+name)
                .catch(function (error) {
                    console.log(error);
                });

            this.getlists()
        },
        parse: function(){
            var now = new Date()
            var date = now.getUTCSeconds();
            var log ="";
            
            var data
            switch(this.selected){
                case 'raw':
                for(var i=0;i<this.sampleLog.split("\n").length;i++){
                log = log +"\\n" + this.sampleLog.split("\n")[i]
            }
                    data = '{"sampleLog":"'+log+'","logType":"'+this.selected+'"}'
                    break
                case 'json':
                    data = '{"sampleLog":"'+btoa(unescape(encodeURIComponent(this.sampleLog)))+'","logType":"'+this.selected+'"}'
                    break
                case 'grok':
                for(var i=0;i<this.sampleLog.split("\n").length;i++){
                log = log +"\\n" + this.sampleLog.split("\n")[i]
            }
                    data = '{"sampleLog":"'+log+'","logType":"'+this.selected+
                    '", "grok_mode":"'+this.grokMulti+'","grok_patterns":"'+btoa(this.grokPatterns)+
                    '","grok_line_head_pattern":"'+btoa(this.headLinePattern)+
                    '","grok_custom_patterns":"'+btoa('\n'+this.customPattern)+ '"}'
                    break
            }
            this.$http.post('/logkit/parse',
                            data, 
                            {emulateJSON:true}).then(response => {
                                // get body data
                                this.points = JSON.stringify(response.body,null,2);
                                this.buildConf()
                            }, response => {
                                // error callback
                                this.points = response.body;
                        });
             
        },
        activeMultiGrok:function(){
            if( this.grokMultiActive == ""){
                this.grokMultiActive = "active"
                this.grokMultiPrimary = "btn-primary"
                this.activeMultiGrokHeadRegexp = ""
                this.grokMulti = "multi"

            }else{
                this.grokMultiActive = ""
                this.grokMultiPrimary = ""
                this.activeMultiGrokHeadRegexp = "none"
                this.grokMulti = ""
            }
            
        },
        buildConf:function(){
            var now = new Date()
            var date = now.getMinutes()+now.getUTCSeconds();
            conf = {"name":this.selected+"-parser-"+date,"type":this.selected,"labels":""}
            switch (this.selected){
                case "grok":
                conf["grok_mode"] = this.grokMulti
                conf["grok_patterns"] = this.grokPatterns
                conf["grok_custom_patterns"] = this.customPattern.split(String.fromCharCode(92)).join(String.fromCharCode(92))
                conf["grok_line_head_pattern"] = this.headLinePattern
                    console.log(conf["grok_line_head_pattern"])

                conf["timezone_offset"]="+08"
                break

            }

            this.parserConfig = '"parser":'+JSON.stringify(conf,null,2).split(String.fromCharCode(92,92)).join(String.fromCharCode(92))
            return conf
        },
        buildSenderConfig:function(){
            var now = new Date()
            var date = now.getMinutes()+now.getUTCSeconds();
            config = {
                "name":"pandora.sender."+date,
                "sender_type":this.senderTypeSelected,
                "pandora_ak":this.pandora_ak, 
                "pandora_sk":this.pandora_sk,
                "pandora_host":this.pandora_host,
                "pandora_repo_name":this.pandora_repo_name,
                "pandora_region":this.pandora_region,
                "pandora_gzip":this.pandora_gzip,
                "pandora_schema_free":"true"
            }
            if (this.flow_rate_limit != ""){
                config["flow_rate_limit"]=this.flow_rate_limit

            }
            this.senderConfig= '"senders":['+JSON.stringify(config,null,2)+"]"
            return config
        },
        buildReaderConfig:function(){
            config = {
                "log_path":this.log_path,
                "mode":this.modeSelected,
                "read_from":this.readFromTypeSelected,
            }

            this.readerConfig= '"reader":'+JSON.stringify(config,null,2)
            return config
        },
        resetNav: function(){
            this.readertabactive=""
            this.parsertabactive=""
            this.sendertabactive=""
            this.configtabactive=""
            this.helptabactive=""
            this.configlistactive=""
        },
        resetView: function(){
            this.parserDisplay="none"
            this.readerDisplay="none"
            this.senderDisplay="none"
            this.configDisplay="none"
            this.helpDisplay="none"
            this.listDisplay = "none"
        },
        nav:function(val){
            this.resetNav()
            this.resetView()
            switch (val){
                case "reader":
                    this.readerDisplay=""
                    this.readertabactive="active"
                    break
                case "parser":
                    this.parserDisplay=""
                    this.parsertabactive="active"
                    break
                case "sender":
                    this.senderDisplay=""
                    this.sendertabactive="active"
                    break  
                case "config":
                    this.configDisplay=""
                    this.configtabactive="active"
                    var now = new Date()
                    var date = now.getMinutes()+now.getUTCSeconds();
                    this.runnerName ="logkit.runner."+date
                    this.runnerConfig='{\n"name":"'+ this.runnerName+'",\n'+this.readerConfig+",\n"+this.parserConfig+",\n"+this.senderConfig+"\n}"
                    break
                case "help":
                    this.helpDisplay=""
                    this.helptabactive="active"
                    break
                case "list":
                    this.getlists()
                    this.listDisplay=""
                    this.configlistactive="active"
                    break
            }
        },
        addRunner: function () {
            try
            {
                runnerObj = JSON.parse(this.runnerConfig)
                this.runnerConfig = JSON.stringify(runnerObj,null,2)
            }
            catch(err)
            {
                console.log(err)
            }
            axios.post("/logkit/configs/"+runnerObj.runnerName,runnerObj)
                .then(function (response) {
                    console.log(response);
                })
                .catch(function (error) {
                    if (error.response) {
                        // The request was made and the server responded with a status code
                        // that falls out of the range of 2xx
                        console.log(error.response);
                    } else if (error.request) {
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
        formatRunnerConfig: function () {
            this.runnerConfig = JSON.stringify(JSON.parse(this.runnerConfig),null,2)
        }
    }

})
ve.getlists()