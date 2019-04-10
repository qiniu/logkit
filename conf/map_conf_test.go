package conf

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetString(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "abc"

	key, err := c.GetString("k1")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, key, "abc")
}

func TestGetStringOr(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "abc"
	key, _ := c.GetStringOr("k2", "def")
	assert.Equal(t, key, "def")
}

func TestGetInt(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "1"

	key, err := c.GetInt("k1")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, key, 1)
}

func TestGetIntOr(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "1"
	key, _ := c.GetIntOr("k2", 2)
	assert.Equal(t, key, 2)
}

func TestGetInt32(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "1"

	key, err := c.GetInt32("k1")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, key, int32(1))
}

func TestGetInt32Or(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "1"
	key, _ := c.GetInt32Or("k2", 2)
	assert.Equal(t, key, int32(2))
}

func TestGetInt64(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "1"

	key, err := c.GetInt64("k1")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, key, int64(1))
}

func TestGetInt64Or(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "1"
	key, _ := c.GetInt64Or("k2", 2)
	assert.Equal(t, key, int64(2))
}

func TestGetBool(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "true"
	key, err := c.GetBool("k1")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, key, true)

}

func TestGetBoolOr(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "true"
	key, _ := c.GetBoolOr("k2", true)
	assert.Equal(t, key, true)
}

func TestGetStringList(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "a,b,c"

	key, err := c.GetStringList("k1")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, len(key), 3)
}

func TestGetStringListOr(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "a,b,c"

	key, _ := c.GetStringListOr("k2", []string{"test"})
	assert.Equal(t, len(key), 1)
}

func TestGetAliasMap(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "a e,b,c"

	key, err := c.GetAliasMap("k1")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, len(key), 3)
	akey, exist := key["a"]
	assert.True(t, exist)
	assert.Equal(t, akey, "e")
	bkey, exist := key["b"]
	assert.True(t, exist)
	assert.Equal(t, bkey, "b")

	key, _ = c.GetAliasMapOr("k2", map[string]string{})
	assert.Equal(t, len(key), 0)
}

func TestGetAliasMapOr(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "a e,b,c"
	key, _ := c.GetAliasMapOr("k2", map[string]string{})
	assert.Equal(t, len(key), 0)
}

func TestGetAliasList(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "a e,b,c"

	key, err := c.GetAliasList("k1")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, len(key), 3)
	name, alias := key[0].Key, key[0].Alias
	assert.Equal(t, name, "a")
	assert.Equal(t, alias, "e")

	name, alias = key[1].Key, key[1].Alias
	assert.Equal(t, name, "b")
	assert.Equal(t, alias, "b")

}

func TestGet(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = ""

	_, err := c.Get("k1")
	if err != nil {
		t.Error(err)
	}
}

func TestGetPasswordEnvString(t *testing.T) {
	t.Parallel()
	err := os.Setenv("TestGetPasswordString", "testPassordString")
	assert.NoError(t, err)
	defer os.Unsetenv("TestGetPasswordString")

	c := MapConf{
		"a": "TestGetPasswordString",
		"b": "${TestGetPasswordString}",
	}

	actual, err := c.GetPasswordEnvString("a")
	assert.NoError(t, err)
	assert.Equal(t, c["a"], actual)

	actual, err = c.GetPasswordEnvString("b")
	assert.NoError(t, err)
	assert.Equal(t, "testPassordString", actual)
}

func TestGetPasswordEnvStringOr(t *testing.T) {
	t.Parallel()
	err := os.Setenv("TestGetPasswordString", "testPassordString")
	assert.NoError(t, err)
	defer os.Unsetenv("TestGetPasswordString")

	c := MapConf{
		"b": "${TestGetPasswordString}",
		"c": "TestGetPasswordString",
	}

	actual, err := c.GetPasswordEnvStringOr("a", "TestGetPasswordString")
	assert.NoError(t, err)
	assert.Equal(t, "TestGetPasswordString", actual)

	actual, err = c.GetPasswordEnvStringOr("a", "${TestGetPasswordString}")
	assert.NoError(t, err)
	assert.Equal(t, "testPassordString", actual)

	actual, err = c.GetPasswordEnvStringOr("b", "${TestGetPasswordStringDeft}")
	assert.NoError(t, err)
	assert.Equal(t, "testPassordString", actual)

	actual, err = c.GetPasswordEnvStringOr("c", "TestGetPasswordStringDeft")
	assert.NoError(t, err)
	assert.Equal(t, c["c"], actual)
}

func TestFunGetStringList(t *testing.T) {
	t.Parallel()
	list := GetStringList("a,b,c")
	assert.Equal(t, len(list), 3)
	assert.EqualValues(t, []string{"a", "b", "c"}, list)
}

func TestGetEnv(t *testing.T) {
	t.Parallel()
	var exceptedValue = "mockEnv"
	err := os.Setenv("test", exceptedValue)
	if err != nil {
		t.Error(err)
	}

	defer os.Clearenv()

	assert.Equal(t, exceptedValue, GetEnv("${test}"))
	assert.Equal(t, exceptedValue, GetEnv("  ${test} "))
}

func TestGetEnvValue(t *testing.T) {
	t.Parallel()
	var exceptedValue = "mockEnv"
	err := os.Setenv("test", exceptedValue)
	if err != nil {
		t.Error(err)
	}

	defer os.Clearenv()

	value, err := GetEnvValue("test")
	assert.NoError(t, err)
	assert.Equal(t, exceptedValue, value)

	value, err = GetEnvValue("  test ")
	assert.NoError(t, err)
	assert.Equal(t, exceptedValue, value)

	value, err = GetEnvValue("tes")
	assert.Error(t, err)
	assert.EqualValues(t, "", value)

	_, err = GetEnvValue("")
	assert.Error(t, err)
	assert.EqualValues(t, "", value)
}

func TestIsEnv(t *testing.T) {
	t.Parallel()
	tests := []struct {
		data          string
		expectIsEnv   bool
		expectEnvName string
	}{
		{},
		{
			data:        "${}",
			expectIsEnv: true,
		},
		{
			data:          "${test1}",
			expectIsEnv:   true,
			expectEnvName: "test1",
		},
		{
			data:          " ${ test1 }",
			expectIsEnv:   true,
			expectEnvName: "test1",
		},
	}
	for _, test := range tests {
		envName, isEnv := IsEnv(test.data)
		assert.Equal(t, test.expectIsEnv, isEnv)
		assert.Equal(t, test.expectEnvName, envName)
	}
}

func TestDeepCopy(t *testing.T) {
	t.Parallel()
	value := MapConf{
		"a": "b",
	}

	result := DeepCopy(value)
	assert.EqualValues(t, value, result)
	value["a"] = "c"
	assert.NotEqual(t, value, result)
}
