package mutate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/utils/models"
)

var (
	testData = "if a new user has a bad time, it's a bug in logkit!!!"
)

func TestRawTransformWithSub(t *testing.T) {
	s := &Sub{
		Start: -1,
		End:   10,
	}
	data, err := s.RawTransform([]string{testData})
	assert.Error(t, err)
	assert.Nil(t, data)

	s = &Sub{
		Start: 15,
		End:   10,
	}
	data, err = s.RawTransform([]string{testData})
	assert.Error(t, err)
	assert.Nil(t, data)

	s = &Sub{
		Start: 6,
		End:   20,
	}
	data, err = s.RawTransform([]string{testData})
	assert.NoError(t, err)
	assert.Equal(t, []string{"ew user has a "}, data)

	s = &Sub{
		Start: 6,
		End:   200,
	}
	data, err = s.RawTransform([]string{testData})
	assert.NoError(t, err)
	assert.Equal(t, []string{"ew user has a bad time, it's a bug in logkit!!!"}, data)

	s = &Sub{
		Start: 150,
		End:   200,
	}
	data, err = s.RawTransform([]string{testData})
	assert.NoError(t, err)
	assert.Equal(t, []string{"if a new user has a bad time, it's a bug in logkit!!!"}, data)
}

func TestTransformWithSub(t *testing.T) {
	s := &Sub{
		Key:   "raw",
		Start: -1,
		End:   10,
	}
	data, err := s.Transform([]models.Data{
		{
			"raw": testData,
		},
	})
	assert.Error(t, err)
	assert.Nil(t, data)

	s = &Sub{
		Key:   "raw",
		Start: 15,
		End:   10,
	}
	data, err = s.Transform([]models.Data{
		{
			"raw": testData,
		},
	})
	assert.Error(t, err)
	assert.Nil(t, data)

	s = &Sub{
		Key:   "raw",
		Start: 6,
		End:   20,
	}
	data, err = s.Transform([]models.Data{
		{
			"raw": testData,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, []models.Data{
		{"raw": "ew user has a "},
	}, data)

	s = &Sub{
		Key:   "raw",
		New:   "new",
		Start: 6,
		End:   200,
	}
	data, err = s.Transform([]models.Data{
		{
			"raw": testData,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, []models.Data{
		{"raw": "if a new user has a bad time, it's a bug in logkit!!!", "new": "ew user has a bad time, it's a bug in logkit!!!"},
	}, data)

	s = &Sub{
		Key:   "raw",
		New:   "new",
		Start: 150,
		End:   200,
	}
	data, err = s.Transform([]models.Data{
		{
			"raw": testData,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, []models.Data{
		{"raw": "if a new user has a bad time, it's a bug in logkit!!!", "new": ""},
	}, data)
}
