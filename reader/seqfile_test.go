package reader

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/qiniu/logkit/reader/test"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	testPidFile          = "abc.pid"
	testQiniuLogFile     = "logkit.log-1115120117"
	testQiniuLogFileTest = "test-logkit.log-1115120117"
	hiddenFile           = ".hidden"
	testlogpath          = "logpath"
)

func Test_Read(t *testing.T) {
	createFile(1000)
	defer DestroyDir()

	meta, err := NewMeta(MetaDir, MetaDir, testlogpath, ModeDir, "", DefautFileRetention)
	if err != nil {
		t.Error(err)
	}
	sf, err := NewSeqFile(meta, Dir, false, false, []string{".pid"}, "*", WhenceOldest)
	if err != nil {
		t.Error(err)
	}
	absDir, err := filepath.Abs(Dir)
	assert.NoError(t, err)
	assert.Equal(t, absDir, filepath.Dir(sf.Source()))
	assert.NotEmpty(t, sf.Name())
	buffer := make([]byte, 5)
	n, err := sf.Read(buffer)
	if n != 5 {
		t.Error("return value must be buffer len 5")
	}
	if err != nil {
		t.Error(err)
	}
	if sf.currFile != filepath.Join(sf.dir, "f3") {
		t.Errorf("current file should be f3: but is %v", sf.currFile)
	}
	n, err = sf.Read(buffer)
	if n != 5 {
		t.Error("return value must be buffer len 5")
	}
	if err != nil {
		t.Error(err)
	}
	if sf.currFile != filepath.Join(sf.dir, "f2") {
		t.Errorf("current file should be f2, but is %v", sf.currFile)
	}
	if buffer[4] != '1' {
		t.Error("the last character should be '1'")
	}
	donefile := sf.meta.DoneFile()
	f, err := os.Open(donefile)
	if err != nil {
		t.Error(err)
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if len(lines) != 1 {
		t.Errorf("done files should be 1, but get %v", len(Files))
	}
	err = scanner.Err()
	if err != nil {
		t.Error(err)
	}

	createPidFile()
	createHiddenFile()

	defer destroyPidFile()
	defer destroyHiddenFile()

	condition := sf.getIgnoreCondition()
	fi, err := os.Stat(testPidFile)
	ignore := condition(fi)
	assert.False(t, ignore)
	fi, err = os.Stat(hiddenFile)
	assert.True(t, condition(fi))
	sf.Close()
}

func Test_NewReaderWithoutFile(t *testing.T) {
	CreateDir()
	meta, err := NewMeta(MetaDir, MetaDir, testlogpath, ModeDir, "", DefautFileRetention)
	if err != nil {
		t.Error(err)
	}
	sf, err := NewSeqFile(meta, Dir, false, false, []string{".pid"}, "*", WhenceOldest)
	if err != nil {
		t.Error(err)
	}
	CreateFiles(1000)
	defer DestroyDir()
	buffer := make([]byte, 9)
	_, err = sf.Read(buffer)
	if err != nil {
		t.Error(err)
	}
	if string(buffer) != "223456789" {
		t.Errorf("exp 223456789 but got %v", string(buffer))
	}
}

func Test_NewReaderWithQiniuLogFile(t *testing.T) {
	CreateDir()
	meta, err := NewMeta(MetaDir, MetaDir, testlogpath, ModeDir, "", DefautFileRetention)
	if err != nil {
		t.Error(err)
	}
	createQiniuLogFile(Dir)
	createInvalidSuffixFile(Dir)
	defer DestroyDir()

	sf, err := NewSeqFile(meta, Dir, false, false, []string{".pid"}, `logkit.log-*`, WhenceOldest)
	if err != nil {
		t.Error(err)
	}
	buffer := make([]byte, 8)
	_, err = sf.Read(buffer)
	if err != nil {
		t.Error(err)
	}
	if string(buffer) != "12345678" {
		t.Errorf("exp 12345678 but got %v", string(buffer))
	}
}

func Test_NewFileNewLine(t *testing.T) {
	CreateDir()
	meta, err := NewMeta(MetaDir, MetaDir, testlogpath, ModeDir, "", DefautFileRetention)
	if err != nil {
		t.Error(err)
	}
	createQiniuLogFile(Dir)
	createInvalidSuffixFile(Dir)
	defer DestroyDir()

	sf, err := NewSeqFile(meta, Dir, false, true, []string{".pid"}, `*`, WhenceOldest)
	if err != nil {
		t.Error(err)
	}
	buffer := make([]byte, 17)
	_, err = sf.Read(buffer)
	if err != nil {
		t.Error(err)
	}
	if string(buffer) != "12345678\n12345678" {
		t.Errorf("exp 12345678\n12345678 but got %v", string(buffer))
	}
}

func Test_NewReaderWithInvalidFile(t *testing.T) {
	CreateDir()
	meta, err := NewMeta(MetaDir, MetaDir, testlogpath, ModeDir, "", DefautFileRetention)
	if err != nil {
		t.Error(err)
	}
	createInvalidSuffixFile(Dir)
	defer DestroyDir()

	sf, err := NewSeqFile(meta, Dir, false, false, []string{".pid"}, `test-logkit.log-*ss`, WhenceOldest)
	if err != nil {
		t.Error(err)
	}

	if sf.currFile != "" {
		t.Errorf("exp emtpy file, but got %s", sf.currFile)
	}
}

func Test_ReadWhenDelete(t *testing.T) {
	createFile(1000)
	defer DestroyDir()

	meta, err := NewMeta(MetaDir, MetaDir, testlogpath, ModeDir, "", DefautFileRetention)
	if err != nil {
		t.Error(err)
	}
	sf, err := NewSeqFile(meta, Dir, false, false, []string{".pid"}, "*", WhenceOldest)
	if err != nil {
		t.Error(err)
	}
	buffer := make([]byte, 5)
	_, err = sf.Read(buffer)
	if err != nil {
		t.Error(err)
	}
	os.Remove(filepath.Join(Dir, "f3"))
	_, err = sf.Read(buffer)
	if err != nil {
		t.Error(err)
	}
	fi, err := os.Stat(sf.currFile)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, fi.Name(), "f2")
}

func Test_ReadNewest(t *testing.T) {
	createFile(1000)
	defer DestroyDir()

	meta, err := NewMeta(MetaDir, MetaDir, testlogpath, ModeDir, "", DefautFileRetention)
	if err != nil {
		t.Error(err)
	}
	sf, err := NewSeqFile(meta, Dir, false, false, []string{".pid"}, "*", WhenceNewest)
	if err != nil {
		t.Error(err)
	}
	buffer := make([]byte, 5)
	n, err := sf.Read(buffer)
	assert.Error(t, err)
	assert.True(t, n == 0)
}

func createHiddenFile() {
	f, _ := os.OpenFile(hiddenFile, os.O_CREATE|os.O_WRONLY, DefaultFilePerm)
	f.WriteString("12345")
	f.Sync()
	f.Close()
}

func destroyHiddenFile() {
	os.RemoveAll(hiddenFile)
}

func createPidFile() {
	f, _ := os.OpenFile(testPidFile, os.O_CREATE|os.O_WRONLY, DefaultFilePerm)
	f.WriteString("12345")
	f.Sync()
	f.Close()
}

func createQiniuLogFile(dirC string) {
	f, _ := os.OpenFile(dirC+"/"+testQiniuLogFile, os.O_CREATE|os.O_WRONLY, DefaultFilePerm)
	f.WriteString("12345678")
	f.Sync()
	f.Close()
}

func createInvalidSuffixFile(dirC string) {
	f, _ := os.OpenFile(dirC+"/"+testQiniuLogFileTest, os.O_CREATE|os.O_WRONLY, DefaultFilePerm)
	f.WriteString("12345678")
	f.Sync()
	f.Close()
}

func destroyPidFile() {
	os.RemoveAll(testPidFile)
}

func Test_SeekUnreachable(t *testing.T) {
	filename := "Test_SeekUnreachable"
	ioutil.WriteFile(filename, []byte("xxx"), os.ModePerm)
	defer os.Remove(filename)
	f, err := os.Open(filename)
	if err != nil {
		t.Error(err)
	}
	x, err := f.Seek(123456, os.SEEK_SET)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(x)
	//ioutil.WriteFile(filename, []byte("yyyy"), os.ModeAppend)
	bx := []byte{}
	n, err := f.Read(bx)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(n, err, string(bx), "xx")
	x1, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("x1", x1)
	st, err := f.Stat()
	fmt.Println(st.Size())
}

func TestLag(t *testing.T) {
	CreateDir()
	meta, err := NewMeta(MetaDir, MetaDir, testlogpath, ModeDir, "", DefautFileRetention)
	if err != nil {
		t.Error(err)
	}
	createQiniuLogFile(Dir)
	createInvalidSuffixFile(Dir)
	defer DestroyDir()

	sf, err := NewSeqFile(meta, Dir, false, false, []string{".pid"}, `logkit.log-*`, WhenceOldest)
	if err != nil {
		t.Error(err)
	}
	buffer := make([]byte, 8)
	_, err = sf.Read(buffer)
	if err != nil {
		t.Error(err)
	}
	if string(buffer) != "12345678" {
		t.Errorf("exp 12345678 but got %v", string(buffer))
	}

	DestroyDir()
	CreateDir()
	rl, err := sf.Lag()
	assert.NoError(t, err)
	assert.Equal(t, &LagInfo{0, "bytes", 0, 0}, rl)
	createQiniuLogFile(Dir)
	createInvalidSuffixFile(Dir)

	rl, err = sf.Lag()
	assert.NoError(t, err)
	assert.Equal(t, &LagInfo{16, "bytes", 0, 0}, rl)
}

func Test_NewFileNewLine2(t *testing.T) {
	createFile(1000)
	defer DestroyDir()

	meta, err := NewMeta(MetaDir, MetaDir, testlogpath, ModeDir, "", DefautFileRetention)
	if err != nil {
		t.Error(err)
	}
	sf, err := NewSeqFile(meta, Dir, false, true, []string{".pid"}, "*", WhenceOldest)
	if err != nil {
		t.Error(err)
	}
	buffer := make([]byte, 9)
	_, err = sf.Read(buffer)
	assert.NoError(t, err)
	assert.Equal(t, Contents[0], string(buffer))

	buffer = make([]byte, 10)
	_, err = sf.Read(buffer)
	assert.NoError(t, err)
	assert.Equal(t, "\n"+Contents[1], string(buffer))

	buffer = make([]byte, 10)
	_, err = sf.Read(buffer)
	assert.NoError(t, err)
	assert.Equal(t, "\n"+Contents[2], string(buffer))

	buffer = make([]byte, 10)
	_, err = sf.Read(buffer)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, string(make([]byte, 10)), string(buffer))
}

func Test_NewFileNewLine3(t *testing.T) {

	meta, err := NewMeta(MetaDir, MetaDir, testlogpath, ModeDir, "", DefautFileRetention)
	if err != nil {
		t.Error(err)
	}
	sf, err := NewSeqFile(meta, Dir, false, true, []string{".pid"}, "*", WhenceOldest)
	if err != nil {
		t.Error(err)
	}

	createFile(1000)
	defer DestroyDir()

	buffer := make([]byte, 9)
	_, err = sf.Read(buffer)
	assert.NoError(t, err)
	assert.Equal(t, Contents[0], string(buffer))

	buffer = make([]byte, 10)
	_, err = sf.Read(buffer)
	assert.NoError(t, err)
	assert.Equal(t, "\n"+Contents[1], string(buffer))

	buffer = make([]byte, 10)
	_, err = sf.Read(buffer)
	assert.NoError(t, err)
	assert.Equal(t, "\n"+Contents[2], string(buffer))

	buffer = make([]byte, 10)
	_, err = sf.Read(buffer)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, string(make([]byte, 10)), string(buffer))
}

func Test_INode(t *testing.T) {

	meta, err := NewMeta(MetaDir, MetaDir, testlogpath, ModeDir, "", DefautFileRetention)
	if err != nil {
		t.Error(err)
	}
	sf, err := NewSeqFile(meta, Dir, false, true, []string{".pid"}, "*", WhenceOldest)
	if err != nil {
		t.Error(err)
	}

	createFile(1000)
	defer DestroyDir()

	buffer := make([]byte, 9)
	_, err = sf.Read(buffer)
	assert.NoError(t, err)
	assert.Equal(t, Contents[0], string(buffer))

	buffer = make([]byte, 10)
	_, err = sf.Read(buffer)
	assert.NoError(t, err)
	assert.Equal(t, "\n"+Contents[1], string(buffer))

	file, err := os.OpenFile(filepath.Join(Dir, "f3"), os.O_WRONLY|os.O_APPEND, DefaultFilePerm)
	assert.NoError(t, err)
	file.WriteString("hello3")
	file.Close()

	buffer = make([]byte, 10)
	_, err = sf.Read(buffer)
	assert.NoError(t, err)
	assert.Equal(t, "\n"+Contents[2], string(buffer))

	buffer = make([]byte, 10)
	_, err = sf.Read(buffer)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, string(make([]byte, 10)), string(buffer))
	err = sf.SyncMeta()
	assert.NoError(t, err)
	err = sf.Close()
	assert.NoError(t, err)

	file, err = os.OpenFile(filepath.Join(Dir, "f1"), os.O_WRONLY|os.O_APPEND, DefaultFilePerm)
	assert.NoError(t, err)
	file.WriteString("hello1")
	file.Close()

	file, err = os.OpenFile(filepath.Join(Dir, "f2"), os.O_WRONLY|os.O_APPEND, DefaultFilePerm)
	assert.NoError(t, err)
	file.WriteString("hello2")
	file.Close()

	sf, err = NewSeqFile(meta, Dir, false, true, []string{".pid"}, "*", WhenceOldest)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 2, len(sf.inodeDone))
	buffer = make([]byte, 6)
	_, err = sf.Read(buffer)
	assert.NoError(t, err)
	assert.Equal(t, "hello1", string(buffer))

	buffer = make([]byte, 10)
	_, err = sf.Read(buffer)
	assert.Equal(t, io.EOF, err)
}
