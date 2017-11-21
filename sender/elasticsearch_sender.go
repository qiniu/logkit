package sender

import (
	"fmt"
	"strings"

	elasticV6 "github.com/olivere/elastic"
	elasticV3 "gopkg.in/olivere/elastic.v3"
	elasticV5 "gopkg.in/olivere/elastic.v5"

	"github.com/qiniu/logkit/conf"

	"context"
	"github.com/qiniu/log"
	"strconv"
	"time"
)

type ElasticsearchSender struct {
	name string

	host      []string
	retention int
	indexName string
	eType     string
	eVersion  string

	aliasFields map[string]string

	intervalIndex int
}

const (
	KeyElasticHost    = "elastic_host"
	KeyElasticVersion = "elastic_version"
	KeyElasticIndex   = "elastic_index"
	KeyElasticType    = "elastic_type"
	KeyElasticAlias   = "elastic_keys"

	KeyElasticIndexStrategy = "index_strategy"
)

const (
	KeyDefaultIndexStrategy = "default"
	KeyYearIndexStrategy    = "year"
	KeyMonthIndexStrategy   = "month"
	KeyDayIndexStrategy     = "day"
)

var (
	ElasticVersion3 = "3.x"
	ElasticVersion5 = "5.x"
	ElasticVersion6 = "6.x"
)

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
	eType, _ := conf.GetStringOr(KeyElasticType, defaultType)
	name, _ := conf.GetStringOr(KeyName, fmt.Sprintf("elasticSender:(elasticUrl:%s,index:%s,type:%s)", host, index, eType))
	fields, _ := conf.GetAliasMapOr(KeyElasticAlias, make(map[string]string))
	eVersion, _ := conf.GetStringOr(KeyElasticVersion, ElasticVersion3)

	strategy := []string{KeyDefaultIndexStrategy, KeyYearIndexStrategy, KeyMonthIndexStrategy, KeyDayIndexStrategy}

	i, err := machPattern(indexStrategy, strategy)
	if err != nil {
		return nil, err
	}

	return &ElasticsearchSender{
		name:          name,
		host:          host,
		indexName:     index,
		eVersion:      eVersion,
		eType:         eType,
		aliasFields:   fields,
		intervalIndex: i,
	}, nil
}

const defaultType string = "logkit"

//判断字符串是否符合已有的模式
func machPattern(s string, strategys []string) (i int, err error) {
	for i, strategy := range strategys {
		if s == strategy {
			return i, err
		}
	}
	err = fmt.Errorf("Unknown index_strategy: '%s'", s)
	return i, err
}

func (this *ElasticsearchSender) Name() string {
	return "//" + this.indexName
}

func (this *ElasticsearchSender) Send(data []Data) (err error) {
	// Create a client
	switch this.eVersion {
	case ElasticVersion6:
		var client *elasticV6.Client
		client, err = elasticV6.NewClient(
			elasticV6.SetSniff(false),
			elasticV6.SetHealthcheck(false),
			elasticV6.SetURL(this.host...))
		if err != nil {
			return
		}
		bulkService := client.Bulk()

		makeDoc := true
		if len(this.aliasFields) == 0 {
			makeDoc = false
		}

		i := this.intervalIndex
		var indexName string
		var intervals []string
		for _, doc := range data {
			indexName = this.indexName
			now := time.Now().UTC()
			intervals = []string{strconv.Itoa(now.Year()), strconv.Itoa(int(now.Month())), strconv.Itoa(now.Day())}
			for j := 1; j <= i; j++ {
				indexName = indexName + "." + intervals[j-1]
			}

			if makeDoc {
				doc = this.wrapDoc(doc)
			}
			doc2 := doc
			bulkService.Add(elasticV6.NewBulkIndexRequest().Index(indexName).Type(this.eType).Doc(&doc2))
		}

		_, err = bulkService.Do(context.Background())
		if err != nil {
			return
		}
	case ElasticVersion5:
		var client *elasticV5.Client
		client, err = elasticV5.NewClient(
			elasticV5.SetSniff(false),
			elasticV5.SetHealthcheck(false),
			elasticV5.SetURL(this.host...))
		if err != nil {
			return
		}
		bulkService := client.Bulk()

		makeDoc := true
		if len(this.aliasFields) == 0 {
			makeDoc = false
		}

		i := this.intervalIndex
		var indexName string
		var intervals []string
		for _, doc := range data {
			indexName = this.indexName
			now := time.Now().UTC()
			intervals = []string{strconv.Itoa(now.Year()), strconv.Itoa(int(now.Month())), strconv.Itoa(now.Day())}
			for j := 1; j <= i; j++ {
				indexName = indexName + "." + intervals[j-1]
			}

			if makeDoc {
				doc = this.wrapDoc(doc)
			}
			doc2 := doc
			bulkService.Add(elasticV5.NewBulkIndexRequest().Index(indexName).Type(this.eType).Doc(&doc2))
		}

		_, err = bulkService.Do(context.Background())
		if err != nil {
			return
		}
	default:
		var client *elasticV3.Client
		client, err = elasticV3.NewClient(elasticV3.SetURL(this.host...))
		if err != nil {
			return
		}
		bulkService := client.Bulk()

		makeDoc := true
		if len(this.aliasFields) == 0 {
			makeDoc = false
		}

		i := this.intervalIndex
		var indexName string
		var intervals []string
		for _, doc := range data {
			indexName = this.indexName
			now := time.Now().UTC()
			intervals = []string{strconv.Itoa(now.Year()), strconv.Itoa(int(now.Month())), strconv.Itoa(now.Day())}
			for j := 1; j <= i; j++ {
				indexName = indexName + "." + intervals[j-1]
			}

			if makeDoc {
				doc = this.wrapDoc(doc)
			}
			doc2 := doc
			bulkService.Add(elasticV3.NewBulkIndexRequest().Index(indexName).Type(this.eType).Doc(&doc2))
		}

		_, err = bulkService.Do()
		if err != nil {
			return
		}
	}

	return
}

func (this *ElasticsearchSender) Close() error {
	return nil
}

func (this *ElasticsearchSender) wrapDoc(doc map[string]interface{}) map[string]interface{} {
	newDoc := make(map[string]interface{})
	for oldKey, newKey := range this.aliasFields {
		val, ok := doc[oldKey]
		if ok {
			newDoc[newKey] = val
			continue
		}
		log.Errorf("key %s not found in doc", oldKey)
	}
	return newDoc
}
