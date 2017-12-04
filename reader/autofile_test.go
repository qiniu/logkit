package reader

import (
	"testing"
	"os"
	
	"github.com/stretchr/testify/assert"
)

func Test_matchModeDir(t *testing.T) {
	var logpathdir string = "/usr/logkit"
	os.Mkdir(logpathdir,defaultDirPerm)
	defer remove(logpathdir)
	modedir, err := matchMode(logpathdir)
	assert.NoError(t, err)
	assert.Equal(t, modedir, ModeDir)
}

func Test_matchModeFile(t *testing.T){
	var logpathfile string = "/usr/logkit/logkit.log"
	os.Create(logpathfile)
	defer remove(logpathfile)
	modefile, err2 := matchMode(logpathfile)
	assert.NoError(t, err2)
	assert.Equal(t, modefile, ModeFile)
}

func Test_matchModeTailx(t *testing.T){
	var logpathtailx string = "/usr/logkit/*/*.log"
	os.Create("/usr/logkit/1/logkit.log")
	defer remove("/usr/logkit/1/logkit.log")
	modetailx, err3 := matchMode(logpathtailx)
	assert.NoError(t, err3)
	assert.Equal(t, modetailx, ModeTailx)
}

func remove(logpath string) {
	os.RemoveAll(logpath)
}