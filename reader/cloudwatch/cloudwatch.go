package cloudwatch

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/json-iterator/go"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/ratelimit"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/utils/models"
)

type (
	CloudWatch struct {
		Region          string
		CollectInterval time.Duration
		Period          time.Duration
		Delay           time.Duration
		Namespace       string
		Metrics         []*Metric
		CacheTTL        time.Duration
		RateLimit       int64
		client          cloudwatchClient
		metricCache     *MetricCache
		meta            *reader.Meta
		status          int32
		StopChan        chan struct{}
		DataChan        chan models.Data
	}

	Metric struct {
		MetricNames []string
		Dimensions  []*Dimension
	}

	Dimension struct {
		Name  string
		Value string
	}

	MetricCache struct {
		TTL     time.Duration
		Fetched time.Time
		Metrics []*cloudwatch.Metric
	}

	cloudwatchClient interface {
		ListMetrics(*cloudwatch.ListMetricsInput) (*cloudwatch.ListMetricsOutput, error)
		GetMetricStatistics(*cloudwatch.GetMetricStatisticsInput) (*cloudwatch.GetMetricStatisticsOutput, error)
	}
)

func init() {
	reader.RegisterConstructor(reader.ModeCloudWatch, NewReader)
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (c reader.Reader, err error) {
	region, err := conf.GetString(reader.KeyRegion)
	if err != nil {
		return
	}
	namespace, err := conf.GetString(reader.KeyNamespace)
	if err != nil {
		return
	}

	ak, _ := conf.GetStringOr(reader.KeyAWSAccessKey, "")
	sk, _ := conf.GetStringOr(reader.KeyAWSSecretKey, "")
	role_arn, _ := conf.GetStringOr(reader.KeyRoleArn, "")
	token, _ := conf.GetStringOr(reader.KeyAWSToken, "")
	profile, _ := conf.GetStringOr(reader.KeyAWSProfile, "")
	sharedCredentialFile, _ := conf.GetStringOr(reader.KeySharedCredentialFile, "")
	credentialConfig := &CredentialConfig{
		Region:    region,
		AccessKey: ak,
		SecretKey: sk,
		RoleARN:   role_arn,
		Profile:   profile,
		Filename:  sharedCredentialFile,
		Token:     token,
	}
	configProvider, err := credentialConfig.Credentials()
	if err != nil {
		log.Errorf("aws Credentials err %v", err)
		return
	}
	cacheTTL, _ := conf.GetStringOr(reader.KeyCacheTTL, "1hr")
	ttl, err := time.ParseDuration(cacheTTL)
	if err != nil {
		err = fmt.Errorf("parse cachettl %v error %v", cacheTTL, err)
		return
	}
	interval, _ := conf.GetStringOr(reader.KeyCollectInterval, "5m")
	collectInteval, err := time.ParseDuration(interval)
	if err != nil {
		err = fmt.Errorf("parse interval %v error %v", interval, err)
		return
	}
	ratelimit, _ := conf.GetInt64Or(reader.KeyRateLimit, 200)
	metrics, _ := conf.GetStringListOr(reader.KeyMetrics, []string{})
	var dimensions []*Dimension
	dimensionList, _ := conf.GetStringListOr(reader.KeyDimension, []string{})
	for _, v := range dimensionList {
		v = strings.TrimSpace(v)
		sks := strings.Fields(v)
		if len(sks) < 1 {
			continue
		}
		var name, value string
		name = sks[0]
		if len(sks) > 1 {
			value = sks[1]
		}
		dimensions = append(dimensions, &Dimension{name, value})
	}
	delayStr, _ := conf.GetStringOr(reader.KeyDelay, "5m")
	delay, err := time.ParseDuration(delayStr)
	if err != nil {
		err = fmt.Errorf("parse delay %v error %v", delayStr, err)
		return
	}
	periodStr, _ := conf.GetStringOr(reader.KeyPeriod, "5m")
	period, err := time.ParseDuration(periodStr)
	if err != nil {
		err = fmt.Errorf("parse period %v error %v", periodStr, err)
		return
	}
	cfg := aws.NewConfig()
	if log.GetOutputLevel() == log.Ldebug {
		cfg.WithLogLevel(aws.LogDebug)
	}
	var Metrics []*Metric
	if len(metrics) > 0 || len(dimensions) > 0 {
		Metrics = []*Metric{{MetricNames: metrics, Dimensions: dimensions}}
	}

	return &CloudWatch{
		Region:          region,
		Namespace:       namespace,
		client:          cloudwatch.New(configProvider, cfg),
		RateLimit:       ratelimit,
		CacheTTL:        ttl,
		Delay:           delay,
		Period:          period,
		meta:            meta,
		CollectInterval: collectInteval,
		status:          reader.StatusInit,
		StopChan:        make(chan struct{}),
		DataChan:        make(chan models.Data),
		Metrics:         Metrics,
	}, nil
}

func (c *CloudWatch) Name() string {
	return "cloudwatch_" + c.Region + "_" + c.Namespace
}

func (c *CloudWatch) Source() string {
	return "cloudwatch_" + c.Region + "_" + c.Namespace
}

func (c *CloudWatch) ReadLine() (line string, err error) {
	if atomic.LoadInt32(&c.status) == reader.StatusInit {
		if err = c.Start(); err != nil {
			log.Error(err)
			return
		}
	}
	select {
	case d := <-c.DataChan:
		var db []byte
		if db, err = jsoniter.Marshal(d); err != nil {
			return
		}
		line = string(db)
	default:
	}
	return
}

func (c *CloudWatch) SetMode(mode string, v interface{}) error {
	return nil
}

func (c *CloudWatch) Close() error {
	close(c.StopChan)
	return nil
}

func (c *CloudWatch) SyncMeta() {}

func isIn(metrics []string, now string) bool {
	for _, v := range metrics {
		if now == v {
			return true
		}
	}
	return false
}

func SelectMetrics(c *CloudWatch) ([]*cloudwatch.Metric, error) {
	var metrics []*cloudwatch.Metric
	// check for provided metric filter
	if len(c.Metrics) > 0 {
		metrics = []*cloudwatch.Metric{}
		for _, m := range c.Metrics {
			if !hasWilcard(m.Dimensions) && len(m.MetricNames) > 0 {
				dimensions := make([]*cloudwatch.Dimension, len(m.Dimensions))
				for k, d := range m.Dimensions {
					dimensions[k] = &cloudwatch.Dimension{
						Name:  aws.String(d.Name),
						Value: aws.String(d.Value),
					}
				}
				for _, name := range m.MetricNames {
					metrics = append(metrics, &cloudwatch.Metric{
						Namespace:  aws.String(c.Namespace),
						MetricName: aws.String(name),
						Dimensions: dimensions,
					})
				}
			} else {
				allMetrics, err := c.fetchNamespaceMetrics()
				if err != nil {
					return nil, err
				}
				if len(m.MetricNames) <= 0 {
					for _, v := range allMetrics {
						if isIn(m.MetricNames, *v.MetricName) {
							continue
						}
						m.MetricNames = append(m.MetricNames, *v.MetricName)
					}
				}
				for _, name := range m.MetricNames {
					log.Debugf("select metric: ", name)
					for _, v := range m.Dimensions {
						log.Debugf("select dimension: ", *v)
					}
					for _, metric := range allMetrics {
						if isSelected(name, metric, m.Dimensions) {
							metrics = append(metrics, &cloudwatch.Metric{
								Namespace:  aws.String(c.Namespace),
								MetricName: aws.String(name),
								Dimensions: metric.Dimensions,
							})
						}
					}
				}
			}
		}
	} else {
		var err error
		metrics, err = c.fetchNamespaceMetrics()
		if err != nil {
			return nil, err
		}
	}
	log.Debugf("get namespace metrics %v", metrics)
	return metrics, nil
}

func (c *CloudWatch) Start() error {
	if !atomic.CompareAndSwapInt32(&c.status, reader.StatusInit, reader.StatusRunning) {
		return fmt.Errorf("runner[%v] Reader[%v] already started", c.meta.RunnerName, c.Name())
	}
	log.Infof("runner[%v] Reader[%v] started", c.meta.RunnerName, c.Name())
	go func() {
		err := c.Gather()
		if err != nil {
			log.Errorf("runner[%v] Reader[%v] err %v ", c.meta.RunnerName, c.Name(), err)
		}
		ticker := time.NewTicker(c.CollectInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := c.Gather()
				if err != nil {
					log.Errorf("runner[%v] Reader[%v] err %v ", c.meta.RunnerName, c.Name(), err)
				}
			case <-c.StopChan:
				close(c.DataChan)
				return
			}
		}
	}()
	return nil
}

func (c *CloudWatch) Gather() error {
	var lastErr error
	metrics, err := SelectMetrics(c)
	if err != nil {
		return err
	}

	now := time.Now()
	lmtr := ratelimit.NewLimiter(c.RateLimit)
	defer lmtr.Close()
	var wg sync.WaitGroup
	wg.Add(len(metrics))
	for _, m := range metrics {
		lmtr.Assign(1)
		go func(inm *cloudwatch.Metric) {
			defer wg.Done()
			datas, err := c.gatherMetric(inm, now)
			if err != nil {
				log.Errorf("gatherMetric error %v", err)
				lastErr = err
			}
			log.Debugf("successfully gatherMetric %v data %v", *inm.MetricName, len(datas))
			for _, v := range datas {
				c.DataChan <- v
			}
		}(m)
	}
	wg.Wait()

	return lastErr
}

/*
 * Fetch available metrics for given CloudWatch Namespace
 */
func (c *CloudWatch) fetchNamespaceMetrics() ([]*cloudwatch.Metric, error) {
	if c.metricCache != nil && c.metricCache.IsValid() {
		return c.metricCache.Metrics, nil
	}

	metrics := []*cloudwatch.Metric{}

	var token *string
	for more := true; more; {
		params := &cloudwatch.ListMetricsInput{
			Namespace:  aws.String(c.Namespace),
			Dimensions: []*cloudwatch.DimensionFilter{},
			NextToken:  token,
			MetricName: nil,
		}

		resp, err := c.client.ListMetrics(params)
		if err != nil {
			return nil, err
		}

		metrics = append(metrics, resp.Metrics...)
		log.Debugf("listMetrics: %v", resp.Metrics)

		token = resp.NextToken
		more = token != nil
	}

	c.metricCache = &MetricCache{
		Metrics: metrics,
		Fetched: time.Now(),
		TTL:     c.CacheTTL,
	}
	return metrics, nil
}

func (c *CloudWatch) gatherMetric(metric *cloudwatch.Metric, now time.Time) (datas []models.Data, err error) {
	params := c.getStatisticsInput(metric, now)
	resp, err := c.client.GetMetricStatistics(params)
	if err != nil {
		return
	}
	log.Debugf("gatherMetric resp %v", resp)
	datas = make([]models.Data, 0, len(resp.Datapoints))

	for _, point := range resp.Datapoints {
		data := make(models.Data)
		for _, d := range metric.Dimensions {
			data[snakeCase(*d.Name)] = *d.Value
		}
		data[reader.KeyTimestamp] = *point.Timestamp

		if point.Average != nil {
			data[formatKey(*metric.MetricName, cloudwatch.StatisticAverage)] = *point.Average
		}
		if point.Maximum != nil {
			data[formatKey(*metric.MetricName, cloudwatch.StatisticMaximum)] = *point.Maximum
		}
		if point.Minimum != nil {
			data[formatKey(*metric.MetricName, cloudwatch.StatisticMinimum)] = *point.Minimum
		}
		if point.SampleCount != nil {
			data[formatKey(*metric.MetricName, cloudwatch.StatisticSampleCount)] = *point.SampleCount
		}
		if point.Sum != nil {
			data[formatKey(*metric.MetricName, cloudwatch.StatisticSum)] = *point.Sum
		}
		datas = append(datas, data)
	}
	return
}

func formatKey(metricName string, statistic string) string {
	return fmt.Sprintf("%s_%s", snakeCase(metricName), snakeCase(statistic))
}

func snakeCase(s string) string {
	s = models.PandoraKey(s)
	s = strings.Replace(s, "__", "_", -1)
	return s
}

/*
 * Map Metric to *cloudwatch.GetMetricStatisticsInput for given timeframe
 */
func (c *CloudWatch) getStatisticsInput(metric *cloudwatch.Metric, now time.Time) *cloudwatch.GetMetricStatisticsInput {
	end := now.Add(-c.Delay)

	input := &cloudwatch.GetMetricStatisticsInput{
		StartTime:  aws.Time(end.Add(-c.Period)),
		EndTime:    aws.Time(end),
		MetricName: metric.MetricName,
		Namespace:  metric.Namespace,
		Period:     aws.Int64(int64(c.Period.Seconds())),
		Dimensions: metric.Dimensions,
		Statistics: []*string{
			aws.String(cloudwatch.StatisticAverage),
			aws.String(cloudwatch.StatisticMaximum),
			aws.String(cloudwatch.StatisticMinimum),
			aws.String(cloudwatch.StatisticSum),
			aws.String(cloudwatch.StatisticSampleCount)},
	}
	return input
}

/*
 * Check Metric Cache validity
 */
func (c *MetricCache) IsValid() bool {
	return c.Metrics != nil && time.Since(c.Fetched) < c.TTL
}

func hasWilcard(dimensions []*Dimension) bool {
	if len(dimensions) <= 0 {
		return true
	}
	for _, d := range dimensions {
		if d.Value == "" || d.Value == "*" {
			return true
		}
	}
	return false
}

func isSelected(name string, metric *cloudwatch.Metric, dimensions []*Dimension) bool {
	log.Debugf("metcis： %v, select: %v real: %v\n", *metric.MetricName, len(dimensions), len(metric.Dimensions))
	for _, v := range metric.Dimensions {
		log.Debugf("dimensions: ", *v)
	}
	if name != *metric.MetricName {
		return false
	}
	// 啥dimension都没写，都选
	if len(dimensions) < 1 {
		return true
	}
	for _, d := range dimensions {
		selected := false
		for _, d2 := range metric.Dimensions {
			if d.Name == *d2.Name {
				if d.Value == "" || d.Value == "*" || d.Value == *d2.Value {
					selected = true
				}
			}
		}
		if !selected {
			return false
		}
	}
	return true
}

type CredentialConfig struct {
	Region    string
	AccessKey string
	SecretKey string
	RoleARN   string
	Profile   string
	Filename  string
	Token     string
}

func (c *CredentialConfig) Credentials() (client.ConfigProvider, error) {
	if c.RoleARN != "" {
		return c.assumeCredentials()
	}
	return c.rootCredentials()
}

func (c *CredentialConfig) rootCredentials() (client.ConfigProvider, error) {
	config := &aws.Config{
		Region: aws.String(c.Region),
	}
	if c.AccessKey != "" || c.SecretKey != "" {
		config.Credentials = credentials.NewStaticCredentials(c.AccessKey, c.SecretKey, c.Token)
	} else if c.Profile != "" || c.Filename != "" {
		config.Credentials = credentials.NewSharedCredentials(c.Filename, c.Profile)
	}
	return session.NewSession(config)
}

func (c *CredentialConfig) assumeCredentials() (client.ConfigProvider, error) {
	rootCredentials, err := c.rootCredentials()
	if err != nil {
		return nil, err
	}
	config := &aws.Config{
		Region: aws.String(c.Region),
	}
	config.Credentials = stscreds.NewCredentials(rootCredentials, c.RoleARN)
	return session.NewSession(config)
}
