package conf

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetString(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "		abc "
	c["k2"] = "abc"
	c["k3"] = " \t "
	c["k4"] = " "

	key, err := c.GetString("k1")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, key, "abc")

	key, err = c.GetString("k2")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, key, "abc")

	key, err = c.GetString("k3")
	assert.Nil(t, err)
	assert.Equal(t, key, " \t ")

	key, err = c.GetString("k4")
	assert.Nil(t, err)
	assert.Equal(t, key, " ")
}

func TestGetStringOr(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "abc"
	key, _ := c.GetStringOr("k1", "def")
	assert.Equal(t, key, "abc")

	key, _ = c.GetStringOr("k2", "def")
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

	c["k1"] = "a"
	key, err = c.GetInt("k1")
	assert.NotNil(t, err)
	assert.Equal(t, 0, key)
}

func TestGetIntOr(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "1"
	key, _ := c.GetIntOr("k1", 2)
	assert.Equal(t, 1, key)

	key, _ = c.GetIntOr("k2", 2)
	assert.Equal(t, 2, key)
}

func TestGetInt32(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "1"

	key, err := c.GetInt32("k1")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, int32(1), key)

	c["k1"] = "a"
	key, err = c.GetInt32("k1")
	assert.NotNil(t, err)
	assert.Equal(t, int32(0), key)
}

func TestGetInt32Or(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "1"
	key, _ := c.GetInt32Or("k1", 2)
	assert.Equal(t, int32(1), key)
	key, _ = c.GetInt32Or("k2", 2)
	assert.Equal(t, int32(2), key)
}

func TestGetInt64(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "1"

	key, err := c.GetInt64("k1")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, int64(1), key)

	c["k1"] = "a"
	key, err = c.GetInt64("k1")
	assert.NotNil(t, err)
	assert.Equal(t, int64(0), key)
}

func TestGetInt64Or(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "1"
	key, _ := c.GetInt64Or("k1", 2)
	assert.Equal(t, int64(1), key)
	key, _ = c.GetInt64Or("k2", 2)
	assert.Equal(t, int64(2), key)
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

	c["k1"] = "a"
	key, err = c.GetBool("k1")
	assert.NotNil(t, err)
	assert.Equal(t, key, false)

}

func TestGetBoolOr(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "true"
	key, _ := c.GetBoolOr("k1", true)
	assert.Equal(t, key, true)
	key, _ = c.GetBoolOr("k2", true)
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
	assert.Equal(t, 3, len(key))

	c["k1"] = ""
	key, err = c.GetStringList("k1")
	assert.NotNil(t, err)
	assert.EqualValues(t, 0, len(key))
}

func TestGetStringListOr(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "a,b,c"

	key, _ := c.GetStringListOr("k1", []string{"test"})
	assert.Equal(t, 3, len(key))
	key, _ = c.GetStringListOr("k2", []string{"test"})
	assert.Equal(t, 1, len(key))
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

	c["k1"] = "a e,   "
	key, err = c.GetAliasMap("k1")
	assert.Nil(t, err)
	assert.EqualValues(t, 1, len(key))

	c["k1"] = "a e,c d e"
	key, err = c.GetAliasMap("k1")
	assert.NotNil(t, err)
	assert.EqualValues(t, 1, len(key))

	c["k1"] = "   ,   "
	key, err = c.GetAliasMap("k1")
	assert.NotNil(t, err)
	assert.EqualValues(t, 0, len(key))
}

func TestGetAliasMapOr(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = "a e,b,c"
	key, _ := c.GetAliasMapOr("k1", map[string]string{})
	assert.Equal(t, 3, len(key))
	key, _ = c.GetAliasMapOr("k2", map[string]string{})
	assert.Equal(t, 0, len(key))
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

	c["k1"] = ""
	key, err = c.GetAliasList("k1")
	assert.NotNil(t, err)
	assert.EqualValues(t, 0, len(key))

	c["k1"] = "a e,"
	key, err = c.GetAliasList("k1")
	assert.Nil(t, err)
	assert.EqualValues(t, 1, len(key))

}

func TestGet(t *testing.T) {
	t.Parallel()
	c := MapConf{}
	c["k1"] = ""

	_, err := c.Get("k1")
	if err != nil {
		t.Error(err)
	}

	_, err = c.Get("k2")
	assert.NotNil(t, err)
}

func TestGetPasswordEnvString(t *testing.T) {
	t.Parallel()
	err := os.Setenv("TestGetPasswordEnvString", "testGetPasswordEnvString")
	assert.NoError(t, err)
	defer os.Unsetenv("TestGetPasswordEnvString")

	c := MapConf{
		"a": "TestGetPasswordEnvString",
		"b": "${TestGetPasswordEnvString}",
		"c": "${}",
	}

	actual, err := c.GetPasswordEnvString("a")
	assert.Nil(t, err)
	assert.Equal(t, c["a"], actual)

	actual, err = c.GetPasswordEnvString("b")
	assert.Nil(t, err)
	assert.Equal(t, "testGetPasswordEnvString", actual)

	actual, err = c.GetPasswordEnvString("c")
	assert.NotNil(t, err)

	_, err = c.GetPasswordEnvString("d")
	assert.NotNil(t, err)
}

func TestGetPasswordEnvStringOr(t *testing.T) {
	t.Parallel()
	err := os.Setenv("TestGetPasswordEnvStringOr", "testPasswordStringOr")
	assert.NoError(t, err)
	defer os.Unsetenv("TestGetPasswordEnvStringOr")

	c := MapConf{
		"b": "${TestGetPasswordEnvStringOr}",
		"c": "TestGetPasswordEnvStringOr",
		"d": "${}",
	}

	actual, err := c.GetPasswordEnvStringOr("a", "TestGetPasswordEnvStringOr")
	assert.Nil(t, err)
	assert.Equal(t, "TestGetPasswordEnvStringOr", actual)

	actual, err = c.GetPasswordEnvStringOr("a", "${TestGetPasswordEnvStringOr}")
	assert.Nil(t, err)
	assert.Equal(t, "testPasswordStringOr", actual)

	actual, err = c.GetPasswordEnvStringOr("b", "${TestGetPasswordStringDeft}")
	assert.Nil(t, err)
	assert.Equal(t, "testPasswordStringOr", actual)

	actual, err = c.GetPasswordEnvStringOr("c", "TestGetPasswordStringDeft")
	assert.Nil(t, err)
	assert.Equal(t, c["c"], actual)

	_, err = c.GetPasswordEnvStringOr("d", "TestGetPasswordStringDeft")
	assert.NotNil(t, err)
}

func TestFunGetStringList(t *testing.T) {
	t.Parallel()
	list := GetStringList("a,b,c")
	assert.Equal(t, len(list), 3)
	assert.EqualValues(t, []string{"a", "b", "c"}, list)
}

func TestGetEnv(t *testing.T) {
	var exceptedValue = "mockEnv"
	err := os.Setenv("TestGetEnv", exceptedValue)
	if err != nil {
		t.Error(err)
	}

	defer os.Clearenv()

	assert.Equal(t, exceptedValue, GetEnv("${TestGetEnv}"))
	assert.Equal(t, exceptedValue, GetEnv("  ${TestGetEnv} "))
	assert.Equal(t, "", GetEnv("  TestGetEnv"))
	assert.Equal(t, "", GetEnv("  ${TestGetEnvNoValue}"))
}

func TestGetEnvValue(t *testing.T) {
	var exceptedValue = "mockEnv"
	err := os.Setenv("TestGetEnvValue", exceptedValue)
	if err != nil {
		t.Error(err)
	}

	defer os.Clearenv()

	value, err := GetEnvValue("TestGetEnvValue")
	assert.NoError(t, err)
	assert.Equal(t, exceptedValue, value)

	value, err = GetEnvValue("  TestGetEnvValue ")
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
