package sender

import (
	"fmt"
	"strings"

	"github.com/qiniu/logkit/conf"

	"github.com/qiniu/log"
	"gopkg.in/olivere/elastic.v3"
)

type ElasticsearchSender struct {
	name string

	host      []string
	retention int
	indexName string
	eType     string

	aliasFields   map[string]string
	elasticClient *elastic.Client
}

const (
	KeyElasticHost  = "elastic_host"
	KeyElasticIndex = "elastic_index"
	KeyElasticType  = "elastic_type"
	KeyElasticAlias = "elastic_keys"
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
	eType, _ := conf.GetStringOr(KeyElasticType, defaultType)
	name, _ := conf.GetStringOr(KeyName, fmt.Sprintf("elasticSender:(elasticUrl:%s,index:%s,type:%s)", host, index, eType))
	fields, _ := conf.GetAliasMapOr(KeyElasticAlias, make(map[string]string))

	return newElasticsearchSender(name, host, index, eType, fields)
}

const defaultType string = "logkit"

func newElasticsearchSender(name string, hosts []string, index, eType string, fields map[string]string) (e *ElasticsearchSender, err error) {

	client, err := elastic.NewClient(elastic.SetURL(hosts...))
	if err != nil {
		return
	}

	exists, err := client.IndexExists(index).Do()
	if err != nil {
		return
	}
	if !exists {
		_, errIn := client.CreateIndex(index).Do()
		if errIn != nil {
			return nil, errIn
		}
	}

	e = &ElasticsearchSender{
		name:          name,
		host:          hosts,
		indexName:     index,
		elasticClient: client,
		eType:         eType,
		aliasFields:   fields,
	}
	return
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
	for _, doc := range data {
		if makeDoc {
			doc = this.wrapDoc(doc)
		}
		bulkService.Add(elastic.NewBulkIndexRequest().Index(this.indexName).Type(this.eType).Doc(&doc))
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
