package cloudwatch

import (
	"errors"
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

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/ratelimit"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ reader.DaemonReader = &Reader{}
	_ reader.DataReader   = &Reader{}
	_ reader.Reader       = &Reader{}
)

type (
	readInfo struct {
		data  Data
		bytes int64
	}

	Reader struct {
		meta *reader.Meta
		// Note: 原子操作，用于表示 reader 整体的运行状态
		status int32
		/*
			Note: 原子操作，用于表示获取数据的线程运行状态

			- StatusInit: 当前没有任务在执行
			- StatusRunning: 当前有任务正在执行
			- StatusStopping: 数据管道已经由上层关闭，执行中的任务完成时直接退出无需再处理
		*/
		routineStatus int32

		stopChan chan struct{} // 用于直接 close 对关闭操作进行广播
		readChan chan readInfo
		errChan  chan error

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

func NewReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {
	region, err := conf.GetString(reader.KeyRegion)
	if err != nil {
		return nil, err
	}
	namespace, err := conf.GetString(reader.KeyNamespace)
	if err != nil {
		return nil, err
	}

	ak, _ := conf.GetStringOr(reader.KeyAWSAccessKey, "")
	sk, _ := conf.GetStringOr(reader.KeyAWSSecretKey, "")
	roleARN, _ := conf.GetStringOr(reader.KeyRoleArn, "")
	token, _ := conf.GetStringOr(reader.KeyAWSToken, "")
	profile, _ := conf.GetStringOr(reader.KeyAWSProfile, "")
	sharedCredentialFile, _ := conf.GetStringOr(reader.KeySharedCredentialFile, "")
	credentialConfig := &CredentialConfig{
		Region:    region,
		AccessKey: ak,
		SecretKey: sk,
		RoleARN:   roleARN,
		Profile:   profile,
		Filename:  sharedCredentialFile,
		Token:     token,
	}
	configProvider, err := credentialConfig.Credentials()
	if err != nil {
		log.Errorf("aws Credentials err %v", err)
		return nil, err
	}
	cacheTTL, _ := conf.GetStringOr(reader.KeyCacheTTL, "1hr")
	ttl, err := time.ParseDuration(cacheTTL)
	if err != nil {
		return nil, fmt.Errorf("parse cachettl %v error %v", cacheTTL, err)
	}
	interval, _ := conf.GetStringOr(reader.KeyCollectInterval, "5m")
	collectInteval, err := time.ParseDuration(interval)
	if err != nil {
		return nil, fmt.Errorf("parse interval %v error %v", interval, err)
	}
	rateLimit, _ := conf.GetInt64Or(reader.KeyRateLimit, 200)
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
		return nil, fmt.Errorf("parse delay %v error %v", delayStr, err)
	}
	periodStr, _ := conf.GetStringOr(reader.KeyPeriod, "5m")
	period, err := time.ParseDuration(periodStr)
	if err != nil {
		return nil, fmt.Errorf("parse period %v error %v", periodStr, err)
	}
	cfg := aws.NewConfig()
	if log.GetOutputLevel() == log.Ldebug {
		cfg.WithLogLevel(aws.LogDebug)
	}
	var Metrics []*Metric
	if len(metrics) > 0 || len(dimensions) > 0 {
		Metrics = []*Metric{{MetricNames: metrics, Dimensions: dimensions}}
	}

	return &Reader{
		meta:            meta,
		status:          reader.StatusInit,
		routineStatus:   reader.StatusInit,
		stopChan:        make(chan struct{}),
		readChan:        make(chan readInfo),
		errChan:         make(chan error),
		Region:          region,
		Namespace:       namespace,
		client:          cloudwatch.New(configProvider, cfg),
		RateLimit:       rateLimit,
		CacheTTL:        ttl,
		Delay:           delay,
		Period:          period,
		CollectInterval: collectInteval,
		Metrics:         Metrics,
	}, nil
}

func (r *Reader) isStopping() bool {
	return atomic.LoadInt32(&r.status) == reader.StatusStopping
}

func (r *Reader) hasStopped() bool {
	return atomic.LoadInt32(&r.status) == reader.StatusStopped
}

func (r *Reader) Name() string {
	return "cloudwatch_" + r.Region + "_" + r.Namespace
}

func (r *Reader) SetMode(mode string, v interface{}) error {
	return nil
}

func (r *Reader) sendError(err error) {
	if err == nil {
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			log.Errorf("Reader %q was panicked and recovered from %v", r.Name(), rec)
		}
	}()
	r.errChan <- err
}

func (r *Reader) Start() error {
	if r.isStopping() || r.hasStopped() {
		return errors.New("reader is stopping or has stopped")
	} else if !atomic.CompareAndSwapInt32(&r.status, reader.StatusInit, reader.StatusRunning) {
		log.Warnf("Runner[%v] %q daemon has already started and is running", r.meta.RunnerName, r.Name())
		return nil
	}

	go func() {
		ticker := time.NewTicker(r.CollectInterval)
		defer ticker.Stop()
		for {
			err := r.Gather()
			if err != nil {
				log.Errorf("Runner[%v] %q gather failed: %v ", r.meta.RunnerName, r.Name(), err)
			}

			select {
			case <-r.stopChan:
				atomic.StoreInt32(&r.status, reader.StatusStopped)
				log.Infof("Runner[%v] %q daemon has stopped from running", r.meta.RunnerName, r.Name())
				return
			case <-ticker.C:
			}
		}
	}()
	log.Infof("Runner[%v] %q daemon has started", r.meta.RunnerName, r.Name())
	return nil
}

func (r *Reader) Source() string {
	return "cloudwatch_" + r.Region + "_" + r.Namespace
}

func (r *Reader) ReadLine() (string, error) {
	return "", errors.New("method ReadLine is not supported, please use ReadData")
}

func (r *Reader) ReadData() (Data, int64, error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case info := <-r.readChan:
		return info.data, info.bytes, nil
	case err := <-r.errChan:
		return nil, 0, err
	case <-timer.C:
	}

	return nil, 0, nil
}

func (_ *Reader) SyncMeta() {}

func (r *Reader) Close() error {
	if !atomic.CompareAndSwapInt32(&r.status, reader.StatusRunning, reader.StatusStopping) {
		log.Warnf("Runner[%v] reader %q is not running, close operation ignored", r.meta.RunnerName, r.Name())
		return nil
	}
	log.Debugf("Runner[%v] %q daemon is stopping", r.meta.RunnerName, r.Name())
	close(r.stopChan)

	// 如果此时没有 routine 正在运行，则在此处关闭数据管道，否则由 routine 在退出时负责关闭
	if atomic.CompareAndSwapInt32(&r.routineStatus, reader.StatusInit, reader.StatusStopping) {
		close(r.readChan)
		close(r.errChan)
	}
	return nil
}

func isIn(metrics []string, now string) bool {
	for _, v := range metrics {
		if now == v {
			return true
		}
	}
	return false
}

func SelectMetrics(c *Reader) ([]*cloudwatch.Metric, error) {
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

func (r *Reader) Gather() error {
	// 未在准备状态（StatusInit）时无法执行此次任务
	if !atomic.CompareAndSwapInt32(&r.routineStatus, reader.StatusInit, reader.StatusRunning) {
		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %q daemon has stopped, this task does not need to be executed and is skipped this time", r.meta.RunnerName, r.Name())
		} else {
			log.Errorf("Runner[%v] %q daemon is still working on last task, this task will not be executed and is skipped this time", r.meta.RunnerName, r.Name())
		}
		return nil
	}
	defer func() {
		// 如果 reader 在 routine 运行时关闭，则需要此 routine 负责关闭数据管道
		if r.isStopping() || r.hasStopped() {
			if atomic.CompareAndSwapInt32(&r.routineStatus, reader.StatusRunning, reader.StatusStopping) {
				close(r.readChan)
				close(r.errChan)
			}
			return
		}
		atomic.StoreInt32(&r.routineStatus, reader.StatusInit)
	}()

	metrics, err := SelectMetrics(r)
	if err != nil {
		return err
	}

	var lastErr error
	now := time.Now()
	lmtr := ratelimit.NewLimiter(r.RateLimit)
	defer lmtr.Close()
	var wg sync.WaitGroup
	wg.Add(len(metrics))
	for _, m := range metrics {
		lmtr.Assign(1)
		go func(inm *cloudwatch.Metric) {
			defer wg.Done()
			datas, err := r.gatherMetric(inm, now)
			if err != nil {
				log.Errorf("gatherMetric error %v", err)
				lastErr = err
				r.sendError(err)
			}
			log.Debugf("successfully gatherMetric %v data %v", *inm.MetricName, len(datas))
			for _, v := range datas {
				r.readChan <- v
			}
		}(m)
	}
	wg.Wait()

	return lastErr
}

/*
 * Fetch available metrics for given CloudWatch Namespace
 */
func (r *Reader) fetchNamespaceMetrics() ([]*cloudwatch.Metric, error) {
	if r.metricCache != nil && r.metricCache.IsValid() {
		return r.metricCache.Metrics, nil
	}

	metrics := []*cloudwatch.Metric{}

	var token *string
	for more := true; more; {
		params := &cloudwatch.ListMetricsInput{
			Namespace:  aws.String(r.Namespace),
			Dimensions: []*cloudwatch.DimensionFilter{},
			NextToken:  token,
			MetricName: nil,
		}

		resp, err := r.client.ListMetrics(params)
		if err != nil {
			return nil, err
		}

		metrics = append(metrics, resp.Metrics...)
		log.Debugf("listMetrics: %v", resp.Metrics)

		token = resp.NextToken
		more = token != nil
	}

	r.metricCache = &MetricCache{
		Metrics: metrics,
		Fetched: time.Now(),
		TTL:     r.CacheTTL,
	}
	return metrics, nil
}

func (r *Reader) gatherMetric(metric *cloudwatch.Metric, now time.Time) ([]readInfo, error) {
	params := r.getStatisticsInput(metric, now)
	resp, err := r.client.GetMetricStatistics(params)
	if err != nil {
		return nil, err
	}
	log.Debugf("gatherMetric resp %v", resp)

	readInfos := make([]readInfo, 0, len(resp.Datapoints))
	for _, point := range resp.Datapoints {
		data := make(Data)
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
		readInfos = append(readInfos, readInfo{data, int64(len(point.String()))})
	}
	return readInfos, nil
}

func formatKey(metricName string, statistic string) string {
	return fmt.Sprintf("%s_%s", snakeCase(metricName), snakeCase(statistic))
}

func snakeCase(s string) string {
	s, _ = PandoraKey(s)
	s = strings.Replace(s, "__", "_", -1)
	return s
}

/*
 * Map Metric to *cloudwatch.GetMetricStatisticsInput for given timeframe
 */
func (r *Reader) getStatisticsInput(metric *cloudwatch.Metric, now time.Time) *cloudwatch.GetMetricStatisticsInput {
	end := now.Add(-r.Delay)

	input := &cloudwatch.GetMetricStatisticsInput{
		StartTime:  aws.Time(end.Add(-r.Period)),
		EndTime:    aws.Time(end),
		MetricName: metric.MetricName,
		Namespace:  metric.Namespace,
		Period:     aws.Int64(int64(r.Period.Seconds())),
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
