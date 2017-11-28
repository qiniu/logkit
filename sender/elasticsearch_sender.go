package sender

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/olivere/elastic.v3"

	"github.com/qiniu/logkit/conf"

	"github.com/qiniu/log"
)

type ElasticsearchSender struct {
	name string

	host           []string
	retention      int
	indexName      string
	eType          string
	logkitSendTime bool
	aliasFields    map[string]string
	elasticClient  *elastic.Client

	intervalIndex int
	timeZone      *time.Location
}

const (
	KeyElasticHost  = "elastic_host"
	KeyElasticIndex = "elastic_index"
	KeyElasticType  = "elastic_type"
	KeyElasticAlias = "elastic_keys"

	KeyElasticIndexStrategy = "elastic_index_strategy"
	KeyElasticTimezone      = "elastic_time_zone"
)

//indexStrategy
const (
	KeyDefaultIndexStrategy = "default"
	KeyYearIndexStrategy    = "year"
	KeyMonthIndexStrategy   = "month"
	KeyDayIndexStrategy     = "day"
)

//timeZone
const (
	KeylocalTimezone = "Local"
	KeyUTCTimezone   = "UTC"
	KeyPRCTimezone   = "PRC"
)

const KeySendTime = "sendTime"

func NewElasticSender(conf conf.MapConf) (sender Sender, err error) {

	host, err := conf.GetStringList(KeyElasticHost)
	if err != nil {
		return
	}
	for i, h := range host {
		if !strings.HasPrefix(h, "http://") {
			host[i] = fmt.Sprintf("http://%s", h)
		}
	}

	index, err := conf.GetString(KeyElasticIndex)
	if err != nil {
		return
	}

	//索引后缀模式
	indexStrategy, _ := conf.GetStringOr(KeyElasticIndexStrategy, KeyDefaultIndexStrategy)
	timezone, _ := conf.GetStringOr(KeyElasticTimezone, KeyUTCTimezone)
	timeZone, err := time.LoadLocation(timezone)
	if err != nil {
		return
	}
	eType, _ := conf.GetStringOr(KeyElasticType, defaultType)
	name, _ := conf.GetStringOr(KeyName, fmt.Sprintf("elasticSender:(elasticUrl:%s,index:%s,type:%s)", host, index, eType))
	fields, _ := conf.GetAliasMapOr(KeyElasticAlias, make(map[string]string))
	logkitSendTime, _ := conf.GetBoolOr(KeyLogkitSendTime, true)
	return newElasticsearchSender(name, host, index, eType, fields, logkitSendTime, indexStrategy, timeZone)
}

const defaultType string = "logkit"

func newElasticsearchSender(name string, hosts []string, index, eType string, fields map[string]string, logkitSendTime bool, indexStrategy string, timeZone *time.Location) (e *ElasticsearchSender, err error) {

	client, err := elastic.NewClient(elastic.SetURL(hosts...))
	if err != nil {
		return
	}

	strategy := []string{KeyDefaultIndexStrategy, KeyYearIndexStrategy, KeyMonthIndexStrategy, KeyDayIndexStrategy}

	i, err := machPattern(indexStrategy, strategy)
	if err != nil {
		return nil, err
	}

	e = &ElasticsearchSender{
		name:           name,
		host:           hosts,
		indexName:      index,
		elasticClient:  client,
		eType:          eType,
		logkitSendTime: logkitSendTime,
		aliasFields:    fields,
		intervalIndex:  i,
		timeZone:       timeZone,
	}
	return
}

//判断字符串是否符合已有的模式
func machPattern(s string, strategys []string) (i int, err error) {
	for i, strategy := range strategys {
		if s == strategy {
			return i, err
		}
	}
	err = fmt.Errorf("Unknown elastic_index_strategy: '%s'", s)
	return i, err
}

func (this *ElasticsearchSender) Name() string {
	return "//" + this.indexName
}

func (this *ElasticsearchSender) Send(data []Data) (err error) {
	client := this.elasticClient

	bulkService := client.Bulk()

	makeDoc := true
	if len(this.aliasFields) == 0 {
		makeDoc = false
	}

	i := this.intervalIndex
	var indexName string
	var intervals []string
	for _, doc := range data {
		//实时计算索引
		indexName = this.indexName
		now := time.Now().In(this.timeZone)
		intervals = []string{strconv.Itoa(now.Year()), strconv.Itoa(int(now.Month())), strconv.Itoa(now.Day())}
		for j := 0; j < i; j++ {
			if j == 0 {
				indexName = indexName + "-" + intervals[j]
			} else {
				if len(intervals[j]) == 1 {
					intervals[j] = "0" + intervals[j]
				}
				indexName = indexName + "." + intervals[j]
			}
		}
		//字段名称替换
		if makeDoc {
			doc = this.wrapDoc(doc)
		}
		//添加发送时间
		if this.logkitSendTime {
			doc[KeySendTime] = now
		}
		doc2 := doc
		bulkService.Add(elastic.NewBulkIndexRequest().Index(indexName).Type(this.eType).Doc(&doc2))
	}

	_, err = bulkService.Do()
	if err != nil {
		return
	}
	return
}

func (this *ElasticsearchSender) Close() error {
	return nil
}

func (this *ElasticsearchSender) wrapDoc(doc map[string]interface{}) map[string]interface{} {
	//newDoc := make(map[string]interface{})
	for oldKey, newKey := range this.aliasFields {
		val, ok := doc[oldKey]
		if ok {
			//newDoc[newKey] = val
			delete(doc, oldKey)
			doc[newKey] = val
			continue
		}
		log.Errorf("key %s not found in doc", oldKey)
	}
	//return newDoc
	return doc
}
