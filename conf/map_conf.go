package conf

import (
	"fmt"
	"strconv"
	"strings"
)

// conf types
const (
	StringType     = "string"
	IntType        = "int"
	Int64Type      = "int64"
	Int32Type      = "int32"
	BoolType       = "bool"
	StringListType = "[]string"
	AliasMapType   = "[string string, string]"
)

// MapConf 基于Map的配置信息
type MapConf map[string]string

type AliasKey struct {
	Key   string
	Alias string
}

func ErrConfMissingKey(key, dataType string) error {
	return fmt.Errorf("MissingKey: The configs must contains %s, dataType must be %s", key, dataType)
}

func ErrConfKeyType(key, dataType string) error {
	return fmt.Errorf("TypeError: The configs must contains %s, dataType must be %s", key, dataType)
}

func ErrMissConfigAliasMap(detailKeys string) error {
	return fmt.Errorf("AliasMapType must use format \"a b\" or \"a\",and split with \",\"")
}

func (conf MapConf) Get(key string) (interface{}, error) {
	value, exist := conf[key]
	if !exist {
		return nil, fmt.Errorf("The configs must contains %s", key)
	}
	return value, nil
}

func (conf MapConf) GetStringOr(key string, deft string) (string, error) {
	ret, err := conf.GetString(key)
	if err != nil || ret == "" {
		return deft, err
	}
	return ret, err
}

func (conf MapConf) GetString(key string) (string, error) {
	value, exist := conf[key]
	if !exist {
		return "", ErrConfMissingKey(key, StringType)
	}
	return value, nil
}

func (conf MapConf) GetIntOr(key string, deft int) (int, error) {
	ret, err := conf.GetInt(key)
	if err != nil {
		return deft, err
	}
	return ret, err
}

func (conf MapConf) GetInt(key string) (int, error) {
	value, exist := conf[key]
	if !exist {
		return 0, ErrConfMissingKey(key, IntType)
	}
	v, err := strconv.Atoi(value)
	if err != nil {
		return 0, ErrConfKeyType(key, IntType)
	}
	return v, nil
}

func (conf MapConf) GetInt32Or(key string, deft int32) (int32, error) {
	ret, err := conf.GetInt32(key)
	if err != nil {
		return deft, err
	}
	return ret, nil
}

func (conf MapConf) GetInt32(key string) (int32, error) {
	value, exist := conf[key]
	if !exist {
		return 0, ErrConfMissingKey(key, Int32Type)
	}
	v, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, ErrConfKeyType(key, Int32Type)
	}
	return int32(v), nil
}

func (conf MapConf) GetInt64Or(key string, deft int64) (int64, error) {
	ret, err := conf.GetInt64(key)
	if err != nil {
		return deft, err
	}
	return ret, nil
}

func (conf MapConf) GetInt64(key string) (int64, error) {
	value, exist := conf[key]
	if !exist {
		return 0, ErrConfMissingKey(key, Int64Type)
	}
	v, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, ErrConfKeyType(key, Int64Type)
	}
	return v, nil
}

func (conf MapConf) GetBoolOr(key string, deft bool) (bool, error) {
	ret, err := conf.GetBool(key)
	if err != nil {
		return deft, err
	}
	return ret, err
}

func (conf MapConf) GetBool(key string) (bool, error) {
	value, exist := conf[key]
	if !exist {
		return false, ErrConfMissingKey(key, BoolType)
	}
	v, err := strconv.ParseBool(value)
	if err != nil {
		return false, ErrConfKeyType(key, BoolType)
	}
	return v, nil
}

func (conf MapConf) GetStringListOr(key string, deft []string) ([]string, error) {
	ret, err := conf.GetStringList(key)
	if err != nil {
		return deft, err
	}
	return ret, err
}

func (conf MapConf) GetStringList(key string) ([]string, error) {
	value, exist := conf[key]
	if !exist {
		return []string{}, ErrConfMissingKey(key, StringListType)
	}
	v := strings.Split(value, ",")
	var newV []string
	for _, i := range v {
		trimI := strings.TrimSpace(i)
		if len(trimI) > 0 {
			newV = append(newV, trimI)
		}
	}
	if len(newV) <= 0 {
		return []string{}, ErrConfKeyType(key, StringListType)
	}
	return newV, nil
}

func (c MapConf) GetAliasList(key string) (aks []AliasKey, err error) {
	ks, err := c.GetStringList(key)
	if err != nil {
		return
	}
	for _, k := range ks {
		parts := strings.Fields(k)
		if len(parts) <= 0 {
			continue
		}
		name := strings.TrimSpace(parts[0])
		alias := name
		if len(parts) >= 2 {
			alias = strings.TrimSpace(parts[1])
		}
		aks = append(aks, AliasKey{Key: name, Alias: alias})
	}
	return
}

func (conf MapConf) GetAliasMapOr(key string, deft map[string]string) (map[string]string, error) {
	ret, err := conf.GetAliasMap(key)
	if err != nil {
		return deft, err
	}
	return ret, err
}

func (conf MapConf) GetAliasMap(key string) (map[string]string, error) {
	value, exist := conf[key]
	if !exist || value == "" {
		return make(map[string]string), ErrConfMissingKey(key, AliasMapType)
	}
	v := strings.Split(value, ",")
	newV := make(map[string]string)
	for _, i := range v {
		trimI := strings.TrimSpace(i)
		if len(trimI) <= 0 {
			continue
		}
		var (
			name  string
			alias string
		)
		splits := strings.Fields(trimI)
		switch len(splits) {
		case 1:
			name, alias = splits[0], splits[0]
		case 2:
			name, alias = splits[0], splits[1]
		default:
			return newV, ErrMissConfigAliasMap(trimI)
		}
		newV[name] = alias
	}
	if len(newV) <= 0 {
		return make(map[string]string), ErrConfKeyType(key, AliasMapType)
	}
	return newV, nil
}
