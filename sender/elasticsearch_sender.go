package sender

import (
	"fmt"
	"strings"

	"gopkg.in/olivere/elastic.v3"

	"github.com/qiniu/logkit/conf"

	"github.com/qiniu/log"

	"errors"
	"time"
	"strconv"
)

type ElasticsearchSender struct {
	name string

	host      []string
	retention int
	indexName string
	eType     string

	aliasFields   map[string]string
	elasticClient *elastic.Client

	intervalIndex int
}

const (
	KeyElasticHost  = "elastic_host"
	KeyElasticIndex = "elastic_index"
	KeyElasticType  = "elastic_type"
	KeyElasticAlias = "elastic_keys"

	KeyElasticIndexInterval = "index_interval"
	defaultIndexinterval = ""
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
	indexInterval, _ := conf.GetStringOr(KeyElasticIndexInterval, defaultIndexinterval)
	eType, _ := conf.GetStringOr(KeyElasticType, defaultType)
	name, _ := conf.GetStringOr(KeyName, fmt.Sprintf("elasticSender:(elasticUrl:%s,index:%s,type:%s)", host, index, eType))
	fields, _ := conf.GetAliasMapOr(KeyElasticAlias, make(map[string]string))

	return newElasticsearchSender(name, host, index, eType, fields, indexInterval)
}

const defaultType string = "logkit"

func newElasticsearchSender(name string, hosts []string, index, eType string, fields map[string]string, indexInterval string) (e *ElasticsearchSender, err error) {

	client, err := elastic.NewClient(elastic.SetURL(hosts...))
	if err != nil {
		return
	}

	/*exists, err := client.IndexExists(index).Do()
	if err != nil {
		return
	}
	if !exists {
		_, errIn := client.CreateIndex(index).Do()
		if errIn != nil {
			return nil, errIn
		}
	}*/

	intervals := []string{"", "year", "month", "day"}

	i := machPattern(indexInterval, intervals)
	if  i == -1{
		err = errors.New("index_interval参数不正确")
		return nil, err
	}

	e = &ElasticsearchSender{
		name:          name,
		host:          hosts,
		indexName:     index,
		elasticClient: client,
		eType:         eType,
		aliasFields:   fields,
		intervalIndex: i,
	}
	return
}

//判断字符串是否符合已有的模式
func machPattern(s string, intervals []string) (i int) {
	for i, pattern := range intervals {
		if s == pattern {
			return i
		}
	}
	return -1
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
		indexName = this.indexName
		now := time.Now().UTC()
		intervals = []string{strconv.Itoa(now.Year()), strconv.Itoa(int(now.Month())), strconv.Itoa(now.Day())}
		for j := 1; j <= i; j ++ {
			indexName = indexName + "." + intervals[j - 1]
		}

		if makeDoc {
			doc = this.wrapDoc(doc)
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