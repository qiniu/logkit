package conf

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	srcconf  = "load_conf_test.conf"
	destconf = "load_conf_test2.conf"
)

func doTestTrimComments(t *testing.T) {

	ast := assert.New(t)
	var (
		err         error
		b, b1       []byte
		conf, conf1 interface{}
	)
	err = LoadEx(&conf, srcconf)
	ast.Nil(err)
	tb, err := ioutil.ReadFile(destconf)
	err = json.Unmarshal(tb, &conf1)
	ast.Nil(err)
	b, err = json.Marshal(conf)
	ast.Nil(err)
	b1, err = json.Marshal(conf1)
	ast.Nil(err)
	ast.Equal(b1, b)
	return
}

func TestDo(t *testing.T) {
	doTestTrimComments(t)
}
