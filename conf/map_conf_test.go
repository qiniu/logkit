package conf

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetString(t *testing.T) {
	c := MapConf{}
	c["k1"] = "abc"

	key, err := c.GetString("k1")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, key, "abc")

	key, _ = c.GetStringOr("k2", "def")
	assert.Equal(t, key, "def")
}

func TestGetInt(t *testing.T) {
	c := MapConf{}
	c["k1"] = "1"

	key, err := c.GetInt("k1")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, key, 1)

	key, _ = c.GetIntOr("k2", 2)
	assert.Equal(t, key, 2)
}

func TestGetBool(t *testing.T) {
	c := MapConf{}
	c["k1"] = "true"

	key, err := c.GetBool("k1")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, key, true)

	key, _ = c.GetBoolOr("k2", true)
	assert.Equal(t, key, true)
}

func TestGetStringList(t *testing.T) {
	c := MapConf{}
	c["k1"] = "a,b,c"

	key, err := c.GetStringList("k1")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, len(key), 3)

	key, _ = c.GetStringListOr("k2", []string{"test"})
	assert.Equal(t, len(key), 1)
}

func TestGetAliasMap(t *testing.T) {
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

func TestGetAliasList(t *testing.T) {
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
	c := MapConf{}
	c["k1"] = ""

	_, err := c.Get("k1")
	if err != nil {
		t.Error(err)
	}
}
