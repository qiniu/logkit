package router

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/qiniu/log"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	//DefaultSenderIndex = 0
	RouterKeyName   = "router_key_name"
	RouterMatchType = "router_match_type"
	//RouterRoutesMap    = "router_routes"
	//RouterMatchValue   = "router_match_value"
	//RouterSenderIndex  = "router_sender_index"
	RouterDefaultIndex = "router_default_sender"

	MTypeEqualName    = "equal"
	MTypeContainsName = "contains"
)

type RouterConfig struct {
	KeyName      string         `json:"router_key_name"`
	MatchType    string         `json:"router_match_type"`
	DefaultIndex int            `json:"router_default_sender"`
	Routes       map[string]int `json:"router_routes"`
}

type Router struct {
	key          string         // 数据中的字段名称
	matchType    mType          // 匹配模式，如 完全相同，包含 等
	defaultIndex int            // 默认 sender
	routes       map[string]int // value1: sender1, value2: sender2
}

func (r *Router) GetSenderIndex(data Data) int {
	if d, exist := data[r.key]; exist {
		for matchValue, index := range r.routes {
			if r.matchType.isMatch(d, matchValue) {
				return index
			}
		}
	}
	return r.defaultIndex
}

func NewSenderRouter(conf RouterConfig, senderCnt int) (*Router, error) {
	keyName := conf.KeyName
	if keyName == "" {
		log.Warnf("route key name is empty, ignored it")
		return nil, nil
	}
	defaultIndex := conf.DefaultIndex
	if defaultIndex >= senderCnt {
		return nil, fmt.Errorf("router default match error, sender %v is not exist", defaultIndex)
	}
	matchTypeName := conf.MatchType
	matchTypeFunc, exist := MatchTypeRegistry[matchTypeName]
	if !exist {
		return nil, fmt.Errorf("router match type error, match Type %v is not support", matchTypeName)
	}
	matchType := matchTypeFunc()

	r := &Router{
		key:          keyName,
		matchType:    matchType,
		defaultIndex: defaultIndex,
	}
	routes := make(map[string]int)
	for val, index := range conf.Routes {
		if index >= senderCnt {
			return nil, fmt.Errorf("router rule error, sender %v is not exist", index)
		}
		routes[val] = index
	}
	r.routes = routes
	return r, nil
}

type MatchType func() mType

var MatchTypeRegistry = map[string]MatchType{}

// 通过这个接口可以拓展匹配的规则
type mType interface {
	name() string
	usage() string
	isMatch(senderValue interface{}, matchValue string) bool
}

func senderValueToString(senderValue interface{}) (string, bool) {
	senderStr := ""
	value := reflect.ValueOf(senderValue)
	switch value.Kind() {
	case reflect.Int64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		senderStr = strconv.FormatInt(value.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		senderStr = strconv.FormatUint(value.Uint(), 10)
	case reflect.Float32:
		senderStr = strconv.FormatFloat(value.Float(), 'f', -1, 32)
	case reflect.Float64:
		senderStr = strconv.FormatFloat(value.Float(), 'f', -1, 64)
	case reflect.String:
		senderStr = value.String()
	default:
		return "", false
	}
	return senderStr, true
}

// 两个值完全相等
type MTypeEqual struct{}

func (e *MTypeEqual) name() string {
	return MTypeEqualName
}

func (e *MTypeEqual) usage() string {
	return "值相等时转发"
}

func (e *MTypeEqual) isMatch(senderValue interface{}, matchValue string) bool {
	if senderStr, ok := senderValueToString(senderValue); ok {
		return senderStr == matchValue
	}
	return false
}

// senderData 中包含
type MTypeContains struct{}

func (c *MTypeContains) name() string {
	return MTypeContainsName
}

func (c *MTypeContains) usage() string {
	return "数据中包含该值时转发"
}

func (c *MTypeContains) isMatch(senderValue interface{}, matchValue string) bool {
	if senderStr, ok := senderValueToString(senderValue); ok {
		return strings.Contains(senderStr, matchValue)
	}
	return false
}

func init() {
	MatchTypeRegistry = map[string]MatchType{
		MTypeEqualName: func() mType {
			return &MTypeEqual{}
		},
		MTypeContainsName: func() mType {
			return &MTypeContains{}
		},
	}
}
