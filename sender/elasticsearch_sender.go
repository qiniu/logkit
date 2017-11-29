package sender

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	elasticV6 "github.com/olivere/elastic"
	elasticV3 "gopkg.in/olivere/elastic.v3"
	elasticV5 "gopkg.in/olivere/elastic.v5"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
)

// ElasticsearchSender ElasticSearch sender
type ElasticsearchSender struct {
	name string

	host            []string
	retention       int
	indexName       string
	eType           string
	eVersion        string
	elasticV3Client *elasticV3.Client
	elasticV5Client *elasticV5.Client
	elasticV6Client *elasticV6.Client

	aliasFields map[string]string

	intervalIndex int
	timeZone      *time.Location
	logkitSendTime bool
}

const (
	KeyElasticHost    = "elastic_host"
	KeyElasticVersion = "elastic_version"
	KeyElasticIndex   = "elastic_index"
	KeyElasticType    = "elastic_type"
	KeyElasticAlias   = "elastic_keys"

	KeyElasticIndexStrategy = "elastic_index_strategy"
	KeyElasticTimezone      = "elastic_time_zone"
)

const (
	KeyDefaultIndexStrategy = "default"
	KeyYearIndexStrategy    = "year"
	KeyMonthIndexStrategy   = "month"
	KeyDayIndexStrategy     = "day"
)

var (
	// ElasticVersion3 v3.x
	ElasticVersion3 = "3.x"
	// ElasticVersion5 v5.x
	ElasticVersion5 = "5.x"
	// ElasticVersion6 v6.x
	ElasticVersion6 = "6.x"
)

//timeZone
const (
	KeylocalTimezone = "Local"
	KeyUTCTimezone   = "UTC"
	KeyPRCTimezone   = "PRC"
)

const KeySendTime = "sendTime"

// NewElasticSender New ElasticSender
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

	// 索引后缀模式
	indexStrategy, _ := conf.GetStringOr(KeyElasticIndexStrategy, KeyDefaultIndexStrategy)
	timezone, _ := conf.GetStringOr(KeyElasticTimezone, KeyUTCTimezone)
	timeZone, err := time.LoadLocation(timezone)
	if err != nil {
		return
	}
	logkitSendTime, _ := conf.GetBoolOr(KeyLogkitSendTime, true)
	eType, _ := conf.GetStringOr(KeyElasticType, defaultType)
	name, _ := conf.GetStringOr(KeyName, fmt.Sprintf("elasticSender:(elasticUrl:%s,index:%s,type:%s)", host, index, eType))
	fields, _ := conf.GetAliasMapOr(KeyElasticAlias, make(map[string]string))
	eVersion, _ := conf.GetStringOr(KeyElasticVersion, ElasticVersion3)

	strategy := []string{KeyDefaultIndexStrategy, KeyYearIndexStrategy, KeyMonthIndexStrategy, KeyDayIndexStrategy}

	i, err := machPattern(indexStrategy, strategy)
	if err != nil {
		return nil, err
	}

	// 初始化 client
	var elasticV3Client *elasticV3.Client
	var elasticV5Client *elasticV5.Client
	var elasticV6Client *elasticV6.Client
	switch eVersion {
	case ElasticVersion6:
		elasticV6Client, err = elasticV6.NewClient(
			elasticV6.SetSniff(false),
			elasticV6.SetHealthcheck(false),
			elasticV6.SetURL(host...))
		if err != nil {
			return
		}
	case ElasticVersion5:
		elasticV5Client, err = elasticV5.NewClient(
			elasticV5.SetSniff(false),
			elasticV5.SetHealthcheck(false),
			elasticV5.SetURL(host...))
		if err != nil {
			return
		}
	default:
		elasticV3Client, err = elasticV3.NewClient(elasticV3.SetURL(host...))
		if err != nil {
			return
		}
	}

	return &ElasticsearchSender{
		name:            name,
		host:            host,
		indexName:       index,
		eVersion:        eVersion,
		elasticV3Client: elasticV3Client,
		elasticV5Client: elasticV5Client,
		elasticV6Client: elasticV6Client,
		eType:           eType,
		aliasFields:     fields,
		intervalIndex:   i,
		timeZone:       timeZone,
		logkitSendTime: logkitSendTime,
	}, nil
}

const defaultType string = "logkit"

// machPattern 判断字符串是否符合已有的模式
func machPattern(s string, strategys []string) (i int, err error) {
	for i, strategy := range strategys {
		if s == strategy {
			return i, err
		}
	}
	err = fmt.Errorf("Unknown index_strategy: '%s'", s)
	return i, err
}

// Name ElasticSearchSenderName
func (ess *ElasticsearchSender) Name() string {
	return "//" + ess.indexName
}

// Send ElasticSearchSender
func (ess *ElasticsearchSender) Send(data []Data) (err error) {
	switch ess.eVersion {
	case ElasticVersion6:
		bulkService := ess.elasticV6Client.Bulk()

		makeDoc := true
		if len(ess.aliasFields) == 0 {
			makeDoc = false
		}

		i := ess.intervalIndex
		var indexName string
		var intervals []string
		for _, doc := range data {
			//实时计算索引
			indexName = ess.indexName
			now := time.Now().In(ess.timeZone)
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
				doc = ess.wrapDoc(doc)
			}
			//添加发送时间
			if ess.logkitSendTime {
				doc[KeySendTime] = now
			}
			doc2 := doc
			bulkService.Add(elasticV6.NewBulkIndexRequest().Index(indexName).Type(ess.eType).Doc(&doc2))
		}

		_, err = bulkService.Do(context.Background())
		if err != nil {
			return
		}
	case ElasticVersion5:
		bulkService := ess.elasticV5Client.Bulk()

		makeDoc := true
		if len(ess.aliasFields) == 0 {
			makeDoc = false
		}

		i := ess.intervalIndex
		var indexName string
		var intervals []string
		for _, doc := range data {
			//实时计算索引
			indexName = ess.indexName
			now := time.Now().In(ess.timeZone)
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
				doc = ess.wrapDoc(doc)
			}
			//添加发送时间
			if ess.logkitSendTime {
				doc[KeySendTime] = now
			}
			doc2 := doc
			bulkService.Add(elasticV5.NewBulkIndexRequest().Index(indexName).Type(ess.eType).Doc(&doc2))
		}

		_, err = bulkService.Do(context.Background())
		if err != nil {
			return
		}
	default:
		bulkService := ess.elasticV3Client.Bulk()

		makeDoc := true
		if len(ess.aliasFields) == 0 {
			makeDoc = false
		}

		i := ess.intervalIndex
		var indexName string
		var intervals []string
		for _, doc := range data {
			//实时计算索引
			indexName = ess.indexName
			now := time.Now().In(ess.timeZone)
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
				doc = ess.wrapDoc(doc)
			}
			//添加发送时间
			if ess.logkitSendTime {
				doc[KeySendTime] = now
			}
			doc2 := doc
			bulkService.Add(elasticV3.NewBulkIndexRequest().Index(indexName).Type(ess.eType).Doc(&doc2))
		}

		_, err = bulkService.Do()
		if err != nil {
			return
		}
	}

	return
}

// Close ElasticSearch Sender Close
func (ess *ElasticsearchSender) Close() error {
	return nil
}

func (ess *ElasticsearchSender) wrapDoc(doc map[string]interface{}) map[string]interface{} {
	//newDoc := make(map[string]interface{})
	for oldKey, newKey := range ess.aliasFields {
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