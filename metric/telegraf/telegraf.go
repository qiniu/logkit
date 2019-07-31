package telegraf

import (
	"fmt"
	"time"

	"github.com/influxdata/telegraf"

	"github.com/qiniu/log"
)

// Accumulator implements telegraf.Accumulator.
type Accumulator struct {
	name     string
	dataSets []map[string]interface{}
	err      error
}

func (acc *Accumulator) AddFields(measurement string,
	fields map[string]interface{},
	tags map[string]string,
	t ...time.Time) {
	data := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		data[acc.name+"_"+k] = v
	}
	acc.dataSets = append(acc.dataSets, data)
}

func (acc *Accumulator) AddGauge(measurement string,
	fields map[string]interface{},
	tags map[string]string,
	t ...time.Time) {
	acc.AddFields(measurement, fields, tags, t...)
}

func (acc *Accumulator) AddCounter(measurement string,
	fields map[string]interface{},
	tags map[string]string,
	t ...time.Time) {
	acc.AddFields(measurement, fields, tags, t...)
}

func (acc *Accumulator) AddSummary(measurement string,
	fields map[string]interface{},
	tags map[string]string,
	t ...time.Time) {
	acc.AddFields(measurement, fields, tags, t...)
}

func (acc *Accumulator) AddHistogram(measurement string,
	fields map[string]interface{},
	tags map[string]string,
	t ...time.Time) {
	acc.AddFields(measurement, fields, tags, t...)
}

func (_ *Accumulator) SetPrecision(precision time.Duration) {
	log.Errorf("Unexpected call of Accumulator.SetPrecision: %v", precision)
}

func (acc *Accumulator) AddError(err error) {
	if acc.err != nil {
		return
	}
	acc.err = err
}

// mock AddMetric. Implement when needed
func (acc *Accumulator) AddMetric(telegraf.Metric) {
}

// mock WithTracking. Implement when needed
func (acc *Accumulator) WithTracking(maxTracked int) telegraf.TrackingAccumulator {
	return nil
}

// Accumulator implements telegraf.Accumulator.
var usages = map[string]string{}

// AddUsage adds usage information to data table.
func AddUsage(name, usage string) {
	usages[name] = usage
}

// GetUsageByName returns usage information specific to logkit.
func GetUsageByName(name string) string {
	usage, has := usages[name]
	if has {
		return usage
	}
	return "usage not found"
}

var configs = map[string]map[string]interface{}{}

// AddConfig adds config information to data table.
func AddConfig(name string, config map[string]interface{}) {
	configs[name] = config
}

// GetConfigByName returns config information specific to logkit.
func GetConfigByName(name string) map[string]interface{} {
	config, has := configs[name]
	if has {
		return config
	}
	return map[string]interface{}{}
}

var tagsTable = map[string][]string{}

// AddTags adds tags information to data table.
func AddTags(name string, tags []string) {
	tagsTable[name] = tags
}

// GetTagsByName returns tags information specific to logkit.
func GetTagsByName(name string) []string {
	tags, has := tagsTable[name]
	if has {
		return tags
	}
	return []string{}
}

// Collector converts inputs.Input to logkit format and implements metric.Collector.
type Collector struct {
	name string
	telegraf.Input
}

// NewCollector creates new general collector.
func NewCollector(name string, input telegraf.Input) *Collector {
	return &Collector{
		name:  name,
		Input: input,
	}
}

func (c *Collector) Name() string {
	return c.name
}

func (c *Collector) Tags() []string {
	return GetTagsByName(c.name)
}

func (c *Collector) Usages() string {
	return GetUsageByName(c.name)
}

func (c *Collector) Config() map[string]interface{} {
	return GetConfigByName(c.name)
}

func (c *Collector) Collect() ([]map[string]interface{}, error) {
	acc := new(Accumulator)
	acc.name = c.name
	if err := c.Gather(acc); err != nil {
		return nil, fmt.Errorf("failed to gather %q from telegraf: %v", c.name, err)
	}

	return acc.dataSets, acc.err
}
