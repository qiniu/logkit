package conf

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	srcConf  = "load_conf_test.conf"
	destConf = "load_conf_test2.conf"
)

func doTestTrimComments(t *testing.T) {
	ast := assert.New(t)
	var (
		err         error
		b, b1       []byte
		conf, conf1 interface{}
	)
	err = LoadEx(&conf, srcConf)
	ast.Nil(err)
	tb, err := ioutil.ReadFile(destConf)
	assert.Nil(t, err)
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

func Test_getEnv(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   []string
		expect string
	}{
		{},
		{
			name:   []string{"name1", "name2", "name3"},
			expect: "name1name2name3",
		},
		{
			name:   []string{"name1"},
			expect: "name1",
		},
	}

	for _, test := range tests {
		for _, n := range test.name {
			os.Setenv(n, n)
		}
		actual := getEnv(test.name)
		assert.EqualValues(t, test.expect, actual)
	}
}

func TestGetConfigDir(t *testing.T) {
	t.Parallel()
	tests := []struct {
		app         string
		homeEnvName string
		expectErr   error
		expectDir   string
	}{
		{
			app:         "",
			homeEnvName: "",
			expectErr:   ErrHomeNotFound,
			expectDir:   "",
		},
		{
			app:         "app",
			homeEnvName: "TEST",
			expectErr:   nil,
			expectDir:   "TEST/.app",
		},
	}

	for _, test := range tests {
		tmp := homeEnvNames
		homeEnvNames = [][]string{{test.homeEnvName}}
		os.Setenv(test.homeEnvName, test.homeEnvName)
		actualDir, actualErr := GetConfigDir(test.app)
		assert.EqualValues(t, test.expectErr, actualErr)
		assert.EqualValues(t, test.expectDir, actualDir)
		homeEnvNames = tmp
	}
}

func TestInit(t *testing.T) {
	tests := []struct {
		cflag       string
		app         string
		defaultConf string
		homeEnvName string
		expect      string
	}{
		{
			expect: "/",
		},
		{
			cflag:       "f",
			app:         "app",
			defaultConf: "defaultConf",
			homeEnvName: "TEST",
			expect:      "TEST/.app/TEST",
		},
	}
	for _, test := range tests {
		tmp := homeEnvNames
		homeEnvNames = [][]string{{test.homeEnvName}}
		os.Setenv(test.homeEnvName, test.homeEnvName)
		Init(test.cflag, test.app, test.homeEnvName)
		assert.NotEmpty(t, *confName)
		homeEnvNames = tmp
	}
}

type Config struct {
	MaxProcs   int      `json:"max_procs"`
	DebugLevel int      `json:"debug_level"`
	ConfsPath  []string `json:"confs_path"`
}

func TestLoad(t *testing.T) {
	tests := []struct {
		content  string
		confName string
		conf     interface{}
		expect   error
	}{
		{
			content: "",
			conf:    &Config{},
			expect:  errors.New("no such file or directory"),
		},
		{
			confName: "./TestLoad",
			content: `{
				"max_procs": 2,
				"debug_level": 1,
				"confs_path": ["conf1","conf2"]
			}`,
			conf: &Config{
				MaxProcs:   2,
				DebugLevel: 1,
				ConfsPath:  []string{"conf1", "conf2"},
			},
			expect: nil,
		},
	}

	for _, test := range tests {
		confName = &test.confName
		if test.confName != "" {
			err := ioutil.WriteFile(test.confName, []byte(test.content), 0600)
			assert.Nil(t, err)
			time.Sleep(3 * time.Second)
		}
		err := Load(test.conf)
		if err != nil {
			t.Log("load conf  ", test.confName, " failed: ", err)
			if test.expect != nil && !strings.Contains(err.Error(), test.expect.Error()) {
				t.Fatalf("expect contains: %s, but got: %s", test.expect.Error(), err.Error())
			}
		}
		os.RemoveAll(test.confName)
	}
}

func TestLoadEx(t *testing.T) {
	t.Parallel()
	tests := []struct {
		content  string
		confName string
		conf     interface{}
		expect   error
	}{
		{
			content: "",
			conf:    &Config{},
			expect:  errors.New("open : no such file or directory"),
		},
		{
			confName: "./TestLoadEx",
			content: `{
"max_procs": 2,
"debug_level": 1,
"confs_path": ["conf1","conf2"]
}`,
			conf: &Config{
				MaxProcs:   2,
				DebugLevel: 1,
				ConfsPath:  []string{"conf1", "conf2"},
			},
			expect: nil,
		},
		{
			confName: "./TestLoadEx",
			content: `{
"max_procs": "2",
"debug_level": 1,
"confs_path": ["conf1","conf2"]
}`,
			conf: &Config{
				MaxProcs:   2,
				DebugLevel: 1,
				ConfsPath:  []string{"conf1", "conf2"},
			},
			expect: errors.New("json: cannot unmarshal string into Go struct field Config.max_procs of type int"),
		},
	}

	for _, test := range tests {
		if test.confName != "" {
			err := ioutil.WriteFile(test.confName, []byte(test.content), 0600)
			assert.Nil(t, err)
		}
		err := LoadEx(test.conf, test.confName)
		assert.EqualValues(t, fmt.Sprintf("%v", test.expect), fmt.Sprintf("%v", err))
		os.RemoveAll(test.confName)
	}
}

func Test_trimComments(t *testing.T) {
	t.Parallel()
	tests := []struct {
		data   []byte
		expect []byte
	}{
		{
			data: []byte(`{
"max_procs": "2",
"debug_level": 1,
"confs_path": ["conf1","conf2"]
}`),
			expect: []byte(`{
"max_procs": "2",
"debug_level": 1,
"confs_path": ["conf1","conf2"]
}`),
		},
	}

	for _, test := range tests {
		actual := trimComments(test.data)
		assert.EqualValues(t, test.expect, actual)
	}

}

func Test_trimCommentsLine(t *testing.T) {
	tests := []struct {
		line   []byte
		expect []byte
	}{
		{
			line: []byte(`{
"max_procs": "2",
"debug_level": 1,
"confs_path": ["conf1","conf2"]
}`),
			expect: []byte(`{
"max_procs": "2",
"debug_level": 1,
"confs_path": ["conf1","conf2"]
}`),
		},
	}

	for _, test := range tests {
		actual := trimCommentsLine(test.line)
		assert.EqualValues(t, test.expect, actual)
	}
}

func TestLoadFile(t *testing.T) {
	t.Parallel()
	confName := "./TestLoadFile"
	conf := &Config{}
	err := ioutil.WriteFile(confName, []byte(`{
"max_procs": 2,
"debug_level": 1,
"confs_path": ["conf1","conf2"]
}`), 0755)
	assert.Nil(t, err)
	defer os.RemoveAll(confName)
	err = LoadFile(conf, confName)
	assert.Nil(t, err)
	assert.EqualValues(t, []string{"conf1", "conf2"}, conf.ConfsPath)
	assert.EqualValues(t, 2, conf.MaxProcs)
	assert.EqualValues(t, 1, conf.DebugLevel)
}
