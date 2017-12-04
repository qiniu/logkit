package reader

import (
	"testing"
	
	"github.com/stretchr/testify/assert"
)

func Test_matchMode(t *testing.T) {
	var logpathdir string = "F:\\apache-tomcat-6.0.18\\log\\"
	modedir, err := matchMode(logpathdir)
	assert.NoError(t, err)
	assert.Equal(t, modedir, ModeDir)
	
	var logpathfile string = "F:\\apache-tomcat-6.0.18\\log\\catalina.log"
	modefile, err2 := matchMode(logpathfile)
	assert.NoError(t, err2)
	assert.Equal(t, modefile, ModeFile)
	
	var logpathtailx string = "F:\\apache-tomcat-6.0.18\\*\\*.log"
	modetailx, err3 := matchMode(logpathfile)
	assert.NoError(t, err3)
	assert.Equal(t, modetailx, ModeTailx)
}