package sender

import (
	"fmt"
	"strings"

	"gopkg.in/olivere/elastic.v3"

	"github.com/qiniu/logkit/conf"

	"github.com/qiniu/log"

	"time"
	"strconv"
	"sync"
	"errors"
)

type ElasticsearchSender struct {
	name string

	host      []string
	retention int
	indexName string
	eType     string

	aliasFields   map[string]string
	elasticClient *elastic.Client
	//新增
	baseIndexName string
	indexInterval string
	c chan string
	rw *sync.RWMutex
}

const (
	KeyElasticHost  = "elastic_host"
	KeyElasticIndex = "elastic_index"
	KeyElasticIndexInterval = "index_interval"
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
	//索引后缀模式
	indexInterval, err := conf.GetString(KeyElasticIndexInterval)
	if err != nil {
		return
	}
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
	e = &ElasticsearchSender{
		name:          name,
		host:          hosts,
		indexName:     index,
		indexInterval: indexInterval,
		elasticClient: client,
		eType:         eType,
		aliasFields:   fields,
	}
	//索引间隔
	intervals := []string{"y", "M", "d", "H", "m"}
	if e.indexInterval == "null" {
		exists, err := client.IndexExists(index).Do()
		if err != nil {
			return nil, err
		}
		if !exists {
			_, errIn := client.CreateIndex(index).Do()
			if errIn != nil {
				return nil, errIn
			}
		}
	}else if i := machPattern(e.indexInterval, intervals); i != -1{
		e.rw = new(sync.RWMutex)
		e.baseIndexName = e.indexName
		e.c = make(chan string, 1)
		startTimer(generateIndex, e, i)
	}else {
		err = errors.New("index_interval参数不正确")
		return nil, err
	}
	return
}

//判断字符串是否符合已有的模式
func machPattern(s string, intervals []string) (i int)  {
	for i, pattern := range intervals{
		if s == pattern{
			return i
		}
	}
	return -1
}

//定时器---定时改变索引
func startTimer(f func(e *ElasticsearchSender, i int), e *ElasticsearchSender, i int){
	log.Infof("%s 开启定时任务,周期创建ES索引:%s, 索引周期 %s", e.name, e.baseIndexName, e.indexInterval)
	addDate := []int{0, 0, 0}
	if i < 3{
		addDate[i] = 1
	}
	go func() {
		for {
			select{
			//接收到关闭信号后,结束循环
			case <- e.c :
				close(e.c)
				log.Infof("%s 关闭定时任务: 周期创建ES索引%s, 索引周期 %s", e.name, e.baseIndexName, e.indexInterval)
				return
				//继续定时任务
			default:
				f(e, i)
				now := time.Now()
				next := now.AddDate(addDate[0], addDate[1], addDate[2])

				if i == 3{
					next = next.Add(time.Hour)
				}else if i == 4{
					next = next.Add(time.Minute)
				}

				date := []int{next.Year(), int(next.Month()), next.Day(), next.Hour(), next.Minute(), next.Second(), next.Nanosecond()}
				for j := i + 1; j <= 6 ; j++{
					date[j] = 0
				}
				next = time.Date(date[0], time.Month(date[1]), date[2], date[3], date[4], date[5], date[6], next.Location())
				t := time.NewTimer(next.Sub(now))
				<-t.C
			}
		}
	}()
}

func generateIndex(e *ElasticsearchSender, i int){
	//改变索引
	time := time.Now()
	year := strconv.Itoa(time.Year())
	month := strconv.Itoa(int(time.Month()))
	day := strconv.Itoa(time.Day())
	hour := strconv.Itoa(time.Hour())
	minute := strconv.Itoa(time.Minute())
	date := []string{year, month, day, hour, minute}
	indexName := e.baseIndexName
	for j := 0; j <= i; j++{
		indexName = indexName + "." + date[j]
	}
	client := e.elasticClient
	//读写锁,防止并发修改异常
	e.rw.Lock()
	e.indexName = indexName
	exists, err := client.IndexExists(e.indexName).Do()
	if err != nil {
		log.Error(err)
	}
	if !exists {
		_, errIn := client.CreateIndex(e.indexName).Do()
		if errIn != nil {
			log.Error(errIn)
		}
		log.Infof("%s 创建ES索引: %s", e.name, e.indexName)
	}
	e.rw.Unlock()
	log.Infof("%s 切换ES索引 %s", e.name, e.indexName)
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
		//原代码,循环中每次传递的都是doc地址(不变),导致数据重复bug
		//bulkService.Add(elastic.NewBulkIndexRequest().Index(this.indexName).Type(this.eType).Doc(&doc))

		//bug修正---新申明一个变量doc2,赋值为doc
		doc2 := doc
		if this.rw != nil{
			this.rw.RLock()
			bulkService.Add(elastic.NewBulkIndexRequest().Index(this.indexName).Type(this.eType).Doc(&doc2))
			this.rw.RUnlock()
		} else {
			bulkService.Add(elastic.NewBulkIndexRequest().Index(this.indexName).Type(this.eType).Doc(&doc2))
		}
	}
	_, err = bulkService.Do()
	//fmt.Println("send---------------------------------------->", this.indexName)
	if err != nil {
		return
	}
	return
}

func (this *ElasticsearchSender) Close() error {
	if this.indexInterval != "null" {
		this.c <- "stop"
	}
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
