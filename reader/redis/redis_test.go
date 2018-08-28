package redis

import (
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
)

func TestRedisReaderWithString(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})

	//put data
	_, err := client.Set("test_str", "bar", 1*time.Minute).Result()
	assert.NoError(t, err)

	conf := conf.MapConf{
		reader.KeyRedisDataType: reader.DataTypeString,
		reader.KeyRedisKey:      "test_str",
	}

	reader, err := NewReader(nil, conf)
	redisReader := reader.(*Reader)
	go func() {
		redisReader.run()
	}()
	assert.Equal(t, "bar", <-redisReader.readChan)
}

func TestRedisReaderWithList(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})

	//put data
	_, err := client.LPush("test_list", "aaa", "bbb", "ccc").Result()
	assert.NoError(t, err)

	conf := conf.MapConf{
		reader.KeyRedisDataType: reader.DataTypeList,
		reader.KeyRedisKey:      "test_list",
	}

	var datas []string
	reader, err := NewReader(nil, conf)
	redisReader := reader.(*Reader)
	go func() {
		for i := 0; i < 3; i++ {
			redisReader.run()
		}
		close(redisReader.readChan)
	}()
	for data, ok := <-redisReader.readChan; ok; data, ok = <-redisReader.readChan {
		datas = append(datas, data)
	}
	assert.Equal(t, []string{"ccc", "bbb", "aaa"}, datas)
}

func TestRedisReaderWithHash(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})

	//put data
	_, err := client.HSet("test_hash", "a", "aaa").Result()
	assert.NoError(t, err)

	conf := conf.MapConf{
		reader.KeyRedisDataType: reader.DataTypeHash,
		reader.KeyRedisKey:      "test_hash",
		reader.KeyRedisHashArea: "a",
	}

	reader, err := NewReader(nil, conf)
	redisReader := reader.(*Reader)
	go func() {
		redisReader.run()
	}()
	assert.Equal(t, "aaa", <-redisReader.readChan)
}

func TestRedisReaderWithSet(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})

	//put data
	_, err := client.SAdd("test_set", "aaa", "bbb", "ccc").Result()
	assert.NoError(t, err)

	conf := conf.MapConf{
		reader.KeyRedisDataType: reader.DataTypeSet,
		reader.KeyRedisKey:      "test_set",
	}

	var datas []string
	reader, err := NewReader(nil, conf)
	redisReader := reader.(*Reader)
	go func() {
		for i := 0; i < 3; i++ {
			redisReader.run()
		}
		close(redisReader.readChan)
	}()
	for data, ok := <-redisReader.readChan; ok; data, ok = <-redisReader.readChan {
		datas = append(datas, data)
	}
	assert.Contains(t, datas, "aaa")
	assert.Contains(t, datas, "bbb")
	assert.Contains(t, datas, "ccc")
}

func TestRedisReaderWithSortedSet(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})

	//put data
	members := []redis.Z{
		{
			Score:  1,
			Member: "aaa",
		},
		{
			Score:  2,
			Member: "bbb",
		},
	}
	_, err := client.ZAdd("test_sortedset", members...).Result()
	assert.NoError(t, err)

	conf := conf.MapConf{
		reader.KeyRedisDataType: reader.DataTypeSortedSet,
		reader.KeyRedisKey:      "test_sortedset",
	}

	var datas []string
	reader, err := NewReader(nil, conf)
	redisReader := reader.(*Reader)
	go func() {
		redisReader.run()
		close(redisReader.readChan)
	}()
	for data, ok := <-redisReader.readChan; ok; data, ok = <-redisReader.readChan {
		datas = append(datas, data)
	}
	assert.Equal(t, []string{"aaa", "bbb"}, datas)
}

func TestNewRedisReader(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	})

	//put data
	_, err := client.Set("test_str", "bar", 1*time.Minute).Result()
	assert.NoError(t, err)

	//check key's type
	con := conf.MapConf{
		reader.KeyRedisDataType: reader.DataTypeString,
		reader.KeyRedisKey:      "test_str",
	}

	_, err = NewReader(nil, con)
	assert.NoError(t, err)

	con = conf.MapConf{
		reader.KeyRedisDataType: reader.DataTypeList,
		reader.KeyRedisKey:      "test_str",
	}
	_, err = NewReader(nil, con)
	assert.Error(t, err)
}
