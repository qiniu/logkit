package elasticsearch

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/json-iterator/go"
	elasticV6 "github.com/olivere/elastic"
	elasticV3 "gopkg.in/olivere/elastic.v3"
	elasticV5 "gopkg.in/olivere/elastic.v5"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/utils/models"
)

var _ sender.SkipDeepCopySender = &Sender{}

// elasticsearch sender
type Sender struct {
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

	intervalIndex  int
	timeZone       *time.Location
	logkitSendTime bool
}

func init() {
	sender.RegisterConstructor(sender.TypeElastic, NewSender)
}

// elasticsearch sender
func NewSender(conf conf.MapConf) (elasticSender sender.Sender, err error) {
	host, err := conf.GetStringList(sender.KeyElasticHost)
	if err != nil {
		return
	}
	for i, h := range host {
		if !strings.HasPrefix(h, "http://") {
			host[i] = fmt.Sprintf("http://%s", h)
		}
	}

	index, err := conf.GetString(sender.KeyElasticIndex)
	if err != nil {
		return
	}

	// 索引后缀模式
	indexStrategy, _ := conf.GetStringOr(sender.KeyElasticIndexStrategy, sender.KeyDefaultIndexStrategy)
	timezone, _ := conf.GetStringOr(sender.KeyElasticTimezone, sender.KeyUTCTimezone)
	timeZone, err := time.LoadLocation(timezone)
	if err != nil {
		return
	}
	logkitSendTime, _ := conf.GetBoolOr(sender.KeyLogkitSendTime, true)
	eType, _ := conf.GetStringOr(sender.KeyElasticType, defaultType)
	name, _ := conf.GetStringOr(sender.KeyName, fmt.Sprintf("elasticSender:(elasticUrl:%s,index:%s,type:%s)", host, index, eType))
	fields, _ := conf.GetAliasMapOr(sender.KeyElasticAlias, make(map[string]string))
	eVersion, _ := conf.GetStringOr(sender.KeyElasticVersion, sender.ElasticVersion3)

	strategy := []string{sender.KeyDefaultIndexStrategy, sender.KeyYearIndexStrategy, sender.KeyMonthIndexStrategy, sender.KeyDayIndexStrategy}

	i, err := matchPattern(indexStrategy, strategy)
	if err != nil {
		return nil, err
	}

	// 初始化 client
	var elasticV3Client *elasticV3.Client
	var elasticV5Client *elasticV5.Client
	var elasticV6Client *elasticV6.Client
	switch eVersion {
	case sender.ElasticVersion6:
		elasticV6Client, err = elasticV6.NewClient(
			elasticV6.SetSniff(false),
			elasticV6.SetHealthcheck(false),
			elasticV6.SetURL(host...))
		if err != nil {
			return
		}
	case sender.ElasticVersion5:
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

	return &Sender{
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
		timeZone:        timeZone,
		logkitSendTime:  logkitSendTime,
	}, nil
}

const defaultType string = "logkit"

// matchPattern 判断字符串是否符合已有的模式
func matchPattern(s string, strategys []string) (i int, err error) {
	for i, strategy := range strategys {
		if s == strategy {
			return i, err
		}
	}
	return i, fmt.Errorf("unknown index_strategy: '%s'", s)
}

// Name ElasticSearchSenderName
func (s *Sender) Name() string {
	return s.indexName
}

// Send ElasticSearchSender
func (s *Sender) Send(datas []Data) error {
	switch s.eVersion {
	case sender.ElasticVersion6:
		bulkService := s.elasticV6Client.Bulk()

		makeDoc := true
		if len(s.aliasFields) == 0 {
			makeDoc = false
		}
		var indexName string
		for _, doc := range datas {
			//计算索引
			indexName = buildIndexName(s.indexName, s.timeZone, s.intervalIndex)
			//字段名称替换
			if makeDoc {
				doc = s.wrapDoc(doc)
			}
			//添加发送时间
			if s.logkitSendTime {
				doc[sender.KeySendTime] = time.Now().In(s.timeZone)
			}
			doc2 := doc
			bulkService.Add(elasticV6.NewBulkIndexRequest().Index(indexName).Type(s.eType).Doc(&doc2))
		}

		resp, err := bulkService.Do(context.Background())
		if err != nil {
			return err
		}

		// 查找出失败的操作并回溯对应的数据返回给上层
		var lastFailedResult *elasticV6.BulkResponseItem
		failedDatas := make([]map[string]interface{}, 0)
		for i, item := range resp.Items {
			for _, result := range item {
				if !(result.Status >= 200 && result.Status <= 299) {
					failedDatas = append(failedDatas, datas[i])
					lastFailedResult = result
					break // 任一情况的失败都算该条数据整体操作失败，没有必要重复检查
				}
			}
		}
		if len(failedDatas) == 0 {
			return nil
		}
		lastError, err := jsoniter.MarshalToString(lastFailedResult)
		if err != nil {
			lastError = fmt.Sprintf("%v", lastFailedResult)
		}
		return reqerr.NewSendError(
			fmt.Sprintf("bulk failed with last error: %s", lastError),
			failedDatas,
			reqerr.TypeBinaryUnpack,
		)

	case sender.ElasticVersion5:
		bulkService := s.elasticV5Client.Bulk()

		makeDoc := true
		if len(s.aliasFields) == 0 {
			makeDoc = false
		}
		var indexName string
		for _, doc := range datas {
			//计算索引
			indexName = buildIndexName(s.indexName, s.timeZone, s.intervalIndex)
			//字段名称替换
			if makeDoc {
				doc = s.wrapDoc(doc)
			}
			//添加发送时间
			if s.logkitSendTime {
				doc[sender.KeySendTime] = time.Now().In(s.timeZone)
			}
			doc2 := doc
			bulkService.Add(elasticV5.NewBulkIndexRequest().Index(indexName).Type(s.eType).Doc(&doc2))
		}

		resp, err := bulkService.Do(context.Background())
		if err != nil {
			return err
		}

		// 查找出失败的操作并回溯对应的数据返回给上层
		var lastFailedResult *elasticV5.BulkResponseItem
		failedDatas := make([]map[string]interface{}, 0)
		for i, item := range resp.Items {
			for _, result := range item {
				if !(result.Status >= 200 && result.Status <= 299) {
					failedDatas = append(failedDatas, datas[i])
					lastFailedResult = result
					break // 任一情况的失败都算该条数据整体操作失败，没有必要重复检查
				}
			}
		}
		if len(failedDatas) == 0 {
			return nil
		}
		lastError, err := jsoniter.MarshalToString(lastFailedResult)
		if err != nil {
			lastError = fmt.Sprintf("%v", lastFailedResult)
		}
		return reqerr.NewSendError(
			fmt.Sprintf("bulk failed with last error: %s", lastError),
			failedDatas,
			reqerr.TypeBinaryUnpack,
		)

	default:
		bulkService := s.elasticV3Client.Bulk()

		makeDoc := true
		if len(s.aliasFields) == 0 {
			makeDoc = false
		}
		var indexName string
		for _, doc := range datas {
			//计算索引
			indexName = buildIndexName(s.indexName, s.timeZone, s.intervalIndex)
			//字段名称替换
			if makeDoc {
				doc = s.wrapDoc(doc)
			}
			//添加发送时间
			if s.logkitSendTime {
				doc[sender.KeySendTime] = time.Now().In(s.timeZone)
			}
			doc2 := doc
			bulkService.Add(elasticV3.NewBulkIndexRequest().Index(indexName).Type(s.eType).Doc(&doc2))
		}

		resp, err := bulkService.Do()
		if err != nil {
			return err
		}

		// 查找出失败的操作并回溯对应的数据返回给上层
		var lastFailedResult *elasticV3.BulkResponseItem
		failedDatas := make([]map[string]interface{}, 0)
		for i, item := range resp.Items {
			for _, result := range item {
				if !(result.Status >= 200 && result.Status <= 299) {
					failedDatas = append(failedDatas, datas[i])
					lastFailedResult = result
					break // 任一情况的失败都算该条数据整体操作失败，没有必要重复检查
				}
			}
		}
		if len(failedDatas) == 0 {
			return nil
		}
		lastError, err := jsoniter.MarshalToString(lastFailedResult)
		if err != nil {
			lastError = fmt.Sprintf("%v", lastFailedResult)
		}
		return reqerr.NewSendError(
			fmt.Sprintf("bulk failed with last error: %s", lastError),
			failedDatas,
			reqerr.TypeBinaryUnpack,
		)
	}
	return nil
}

func buildIndexName(indexName string, timeZone *time.Location, size int) string {
	now := time.Now().In(timeZone)
	intervals := []string{strconv.Itoa(now.Year()), strconv.Itoa(int(now.Month())), strconv.Itoa(now.Day())}
	for j := 0; j < size; j++ {
		if j == 0 {
			indexName = indexName + "-" + intervals[j]
		} else {
			if len(intervals[j]) == 1 {
				intervals[j] = "0" + intervals[j]
			}
			indexName = indexName + "." + intervals[j]
		}
	}
	return indexName
}

// Close ElasticSearch Sender Close
func (_ *Sender) Close() error { return nil }

func (s *Sender) wrapDoc(doc map[string]interface{}) map[string]interface{} {
	for oldKey, newKey := range s.aliasFields {
		val, ok := doc[oldKey]
		if ok {
			delete(doc, oldKey)
			doc[newKey] = val
			continue
		}
		log.Errorf("key %s not found in doc", oldKey)
	}
	return doc
}

func (_ *Sender) SkipDeepCopy() bool { return true }
