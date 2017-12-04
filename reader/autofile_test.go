package reader

import (
	"testing"
	"os"
	"fmt"
	
	"github.com/stretchr/testify/assert"
)

func Test_matchModeDir(t *testing.T) {
	var logpathdir string = os.TempDir() + "logkit"
	os.Mkdir(logpathdir,0755)
	defer remove(logpathdir)
	modedir, err := matchMode(logpathdir)
	fmt.Println(modedir)
	assert.NoError(t, err)
	assert.Equal(t, modedir, ModeDir)
}

func Test_matchModeFile(t *testing.T){
	var logpathfile string = os.TempDir() + "logkit.log"
	os.Create(logpathfile)
	defer remove(logpathfile)
	modefile, err2 := matchMode(logpathfile)
	assert.NoError(t, err2)
	assert.Equal(t, modefile, ModeFile)
}

func Test_matchModeTailx(t *testing.T){
	var logpathtailx string = "/logkit/*/*.log"
	os.Create(os.TempDir() + "logkit/1/logkit.log")
	defer remove(os.TempDir() + "logkit/1/logkit.log")
	modetailx, err3 := matchMode(logpathtailx)
	fmt.Println(modetailx)
	assert.NoError(t, err3)
	assert.Equal(t, modetailx, ModeTailx)
}

func remove (logpath string) {
	os.RemoveAll(logpath)
}