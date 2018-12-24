package elasticsearch

import (
	"context"
	"fmt"
	"net/http"
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
	. "github.com/qiniu/logkit/sender/config"
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
	sender.RegisterConstructor(TypeElastic, NewSender)
}

// elasticsearch sender
func NewSender(conf conf.MapConf) (elasticSender sender.Sender, err error) {
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
	eVersion, _ := conf.GetStringOr(KeyElasticVersion, ElasticVersion5)

	strategy := []string{KeyDefaultIndexStrategy, KeyYearIndexStrategy, KeyMonthIndexStrategy, KeyDayIndexStrategy}

	i, err := matchPattern(indexStrategy, strategy)
	if err != nil {
		return nil, err
	}

	authUsername, _ := conf.GetStringOr(KeyAuthUsername, "")
	authPassword, _ := conf.GetPasswordEnvStringOr(KeyAuthPassword, "")
	enableGzip, _ := conf.GetBoolOr(KeyEnableGzip, false)

	// 初始化 client
	var elasticV3Client *elasticV3.Client
	var elasticV5Client *elasticV5.Client
	var elasticV6Client *elasticV6.Client
	switch eVersion {
	case ElasticVersion6:
		optFns := []elasticV6.ClientOptionFunc{
			elasticV6.SetSniff(false),
			elasticV6.SetHealthcheck(false),
			elasticV6.SetURL(host...),
			elasticV6.SetGzip(enableGzip),
		}

		if len(authUsername) > 0 && len(authPassword) > 0 {
			optFns = append(optFns, elasticV6.SetBasicAuth(authUsername, authPassword))
		}

		elasticV6Client, err = elasticV6.NewClient(optFns...)
		if err != nil {
			return nil, err
		}
	case ElasticVersion3:
		optFns := []elasticV3.ClientOptionFunc{
			elasticV3.SetSniff(false),
			elasticV3.SetHealthcheck(false),
			elasticV3.SetURL(host...),
			elasticV3.SetGzip(enableGzip),
		}

		if len(authUsername) > 0 && len(authPassword) > 0 {
			optFns = append(optFns, elasticV3.SetBasicAuth(authUsername, authPassword))
		}

		elasticV3Client, err = elasticV3.NewClient(optFns...)
		if err != nil {
			return nil, err
		}
	default:
		httpClient := &http.Client{
			Timeout: 300 * time.Second,
		}
		optFns := []elasticV5.ClientOptionFunc{
			elasticV5.SetSniff(false),
			elasticV5.SetHealthcheck(false),
			elasticV5.SetURL(host...),
			elasticV5.SetGzip(enableGzip),
			elasticV5.SetHttpClient(httpClient),
		}

		if len(authUsername) > 0 && len(authPassword) > 0 {
			optFns = append(optFns, elasticV5.SetBasicAuth(authUsername, authPassword))
		}

		elasticV5Client, err = elasticV5.NewClient(optFns...)
		if err != nil {
			return nil, err
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
	case ElasticVersion6:
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
				doc[KeySendTime] = time.Now().In(s.timeZone).UnixNano() / 1000000
			}
			doc2 := doc
			bulkService.Add(elasticV6.NewBulkIndexRequest().UseEasyJSON(true).Index(indexName).Type(s.eType).Doc(&doc2))
		}

		resp, err := bulkService.Do(context.Background())
		if err != nil {
			return err
		}

		var (
			// 查找出失败的操作并回溯对应的数据返回给上层
			lastFailedResult *elasticV6.BulkResponseItem
			failedDatas      = make([]map[string]interface{}, len(datas))
			failedDatasIdx   = 0
		)
		for i, item := range resp.Items {
			for _, result := range item {
				if !(result.Status >= 200 && result.Status <= 299) {
					failedDatas[failedDatasIdx] = datas[i]
					failedDatasIdx++
					lastFailedResult = result
					break // 任一情况的失败都算该条数据整体操作失败，没有必要重复检查
				}
			}
		}
		failedDatas = failedDatas[:failedDatasIdx]
		if len(failedDatas) == 0 {
			return nil
		}
		lastError, err := jsoniter.MarshalToString(lastFailedResult)
		if err != nil {
			lastError = fmt.Sprintf("marshal to string failed: %v", lastFailedResult)
		}

		return &StatsError{
			StatsInfo: StatsInfo{
				Success:   int64(len(datas) - len(failedDatas)),
				Errors:    int64(len(failedDatas)),
				LastError: lastError,
			},
			SendError: reqerr.NewSendError(
				fmt.Sprintf("bulk failed with last error: %s", lastError),
				failedDatas,
				reqerr.TypeBinaryUnpack,
			),
		}

	case ElasticVersion5:
		bulkService := s.elasticV5Client.Bulk()

		makeDoc := true
		if len(s.aliasFields) == 0 {
			makeDoc = false
		}
		curTime := time.Now().In(s.timeZone).UnixNano() / 1000000
		indexName := buildIndexName(s.indexName, s.timeZone, s.intervalIndex)
		for _, doc := range datas {
			//计算索引
			//字段名称替换
			if makeDoc {
				doc = s.wrapDoc(doc)
			}
			//添加发送时间
			if s.logkitSendTime {
				doc[KeySendTime] = curTime
			}
			doc2 := doc
			bulkService.Add(elasticV5.NewBulkIndexRequest().Index(indexName).Type(s.eType).Doc(&doc2))
		}

		resp, err := bulkService.Do(context.Background())
		if err != nil {
			return err
		}

		var (
			// 查找出失败的操作并回溯对应的数据返回给上层
			lastFailedResult *elasticV5.BulkResponseItem
			failedDatas      = make([]map[string]interface{}, len(datas))
			failedDatasIdx   = 0
		)
		for i, item := range resp.Items {
			for _, result := range item {
				if !(result.Status >= 200 && result.Status <= 299) {
					failedDatas[failedDatasIdx] = datas[i]
					failedDatasIdx++
					lastFailedResult = result
					break // 任一情况的失败都算该条数据整体操作失败，没有必要重复检查
				}
			}
		}
		failedDatas = failedDatas[:failedDatasIdx]
		if len(failedDatas) == 0 {
			return nil
		}
		lastError, err := jsoniter.MarshalToString(lastFailedResult)
		if err != nil {
			lastError = fmt.Sprintf("marshal to string failed: %v", lastFailedResult)
		}
		return &StatsError{
			StatsInfo: StatsInfo{
				Success:   int64(len(datas) - len(failedDatas)),
				Errors:    int64(len(failedDatas)),
				LastError: lastError,
			},
			SendError: reqerr.NewSendError(
				fmt.Sprintf("bulk failed with last error: %s", lastError),
				failedDatas,
				reqerr.TypeBinaryUnpack,
			),
		}

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
				doc[KeySendTime] = time.Now().In(s.timeZone).UnixNano() / 1000000
			}
			doc2 := doc
			bulkService.Add(elasticV3.NewBulkIndexRequest().Index(indexName).Type(s.eType).Doc(&doc2))
		}

		resp, err := bulkService.Do()
		if err != nil {
			return err
		}

		var (
			// 查找出失败的操作并回溯对应的数据返回给上层
			lastFailedResult *elasticV3.BulkResponseItem
			failedDatas      = make([]map[string]interface{}, len(datas))
			failedDatasIdx   = 0
		)
		for i, item := range resp.Items {
			for _, result := range item {
				if !(result.Status >= 200 && result.Status <= 299) {
					failedDatas[failedDatasIdx] = datas[i]
					failedDatasIdx++
					lastFailedResult = result
					break // 任一情况的失败都算该条数据整体操作失败，没有必要重复检查
				}
			}
		}
		failedDatas = failedDatas[:failedDatasIdx]
		if len(failedDatas) == 0 {
			return nil
		}
		lastError, err := jsoniter.MarshalToString(lastFailedResult)
		if err != nil {
			lastError = fmt.Sprintf("marshal to string failed: %v", lastFailedResult)
		}
		return &StatsError{
			StatsInfo: StatsInfo{
				Success:   int64(len(datas) - len(failedDatas)),
				Errors:    int64(len(failedDatas)),
				LastError: lastError,
			},
			SendError: reqerr.NewSendError(
				fmt.Sprintf("bulk failed with last error: %s", lastError),
				failedDatas,
				reqerr.TypeBinaryUnpack,
			),
		}
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
func (s *Sender) Close() error {
	if s.elasticV3Client != nil {
		s.elasticV3Client.Stop()
	}
	if s.elasticV5Client != nil {
		s.elasticV5Client.Stop()
	}
	if s.elasticV6Client != nil {
		s.elasticV6Client.Stop()
	}
	return nil
}

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
