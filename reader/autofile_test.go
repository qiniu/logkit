package reader

import (
	"testing"
	"os"
	
	"github.com/stretchr/testify/assert"
)

func Test_matchModeDir(t *testing.T) {
	var logpathdir string = "/usr/"
	modedir, err := matchMode(logpathdir)
	assert.NoError(t, err)
	assert.Equal(t, modedir, ModeDir)
}

func Test_matchModeFile(t *testing.T){
	var logpathfile string = "/usr/logkit.log"
	modefile, err2 := matchMode(logpathfile)
	assert.NoError(t, err2)
	assert.Equal(t, modefile, ModeFile)
}

func Test_matchModeTailx(t *testing.T){
	var logpathtailx string = "/usr/*/*.log"
	modetailx, err3 := matchMode(logpathtailx)
	assert.NoError(t, err3)
	assert.Equal(t, modetailx, ModeTailx)
}