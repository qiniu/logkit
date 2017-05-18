package reader

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/utils"

	"github.com/qiniu/log"
)

var dir = "logdir"
var metaDir = "./meta"
var files = []string{"f3", "f2", "f1"}
var contents = []string{"223456789", "123456789", "123456789"}

func createFile(interval int) {
	createDir()
	createOnlyFiles(interval)
}

func createDir() {
	err := os.Mkdir(dir, 0755)
	if err != nil {
		log.Error(err)
		return
	}
}

func createOnlyFiles(interval int) {
	for i, f := range files {
		file, err := os.OpenFile(filepath.Join(dir, f), os.O_CREATE|os.O_WRONLY, defaultFilePerm)
		if err != nil {
			log.Error(err)
			return
		}

		file.WriteString(contents[i])
		file.Close()
		time.Sleep(time.Millisecond * time.Duration(interval))
	}
}

func destroyFile() {
	os.RemoveAll(dir)
	os.RemoveAll(metaDir)
}

func TestMeta(t *testing.T) {
	createFile(50)
	defer destroyFile()
	logkitConf := conf.MapConf{
		KeyMetaPath: metaDir,
		KeyFileDone: metaDir,
		KeyLogPath:  dir,
		KeyMode:     ModeDir,
	}
	meta, err := NewMetaWithConf(logkitConf)
	if err != nil {
		t.Error(err)
	}
	os.RemoveAll(metaDir)
	// no metaDir conf except work
	confNoMetaPath := conf.MapConf{
		KeyLogPath:          dir,
		utils.GlobalKeyName: "mock_runner_name",
		KeyMode:             ModeDir,
	}
	meta, err = NewMetaWithConf(confNoMetaPath)
	if err != nil {
		t.Error(err)
	}
	f, err := os.Stat("meta/" + "mock_runner_name_" + hash(dir))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(f.Name(), hash(dir)) {
		t.Fatal("not excepted dir")
	}
	dirToRm := "meta"
	os.RemoveAll(dirToRm)

	meta, err = NewMeta(metaDir, metaDir, ModeDir, "logpath", 7)
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(metaDir)

	file, offset, err := meta.ReadOffset()
	if err == nil {
		t.Error("offset must be nil")
	}
	err = meta.WriteOffset(filepath.Join(dir, "f1"), 2)
	if err != nil {
		t.Error(err)
	}
	file, offset, err = meta.ReadOffset()
	if file != filepath.Join(dir, "f1") {
		t.Error("file should be " + filepath.Join(dir, "f1"))
	}
	if offset != 2 {
		t.Error("file offset should be 2")
	}
	err = meta.AppendDoneFile("f1")
	if err != nil {
		t.Error(err)
	}
	if meta.IsDoneFile("file.done.12212") != true {
		t.Error("test is done file error,expect file.done.12212 is done file")
	}
	if meta.IsDoneFile("filedone.11111") != false {
		t.Error("test is done file error,expect filedone.11111 is done file")
	}
	donefile := filepath.Join(metaDir, doneFileName+".2015-11-11")
	_, err = os.Create(donefile)
	if err != nil {
		t.Error(err)
	}
	if err = meta.DeleteDoneFile(donefile); err != nil {
		t.Error(err)
	}
	_, err = os.Stat(filepath.Join(metaDir, deletedFileName+".2015-11-11"))
	if err == nil || !os.IsNotExist(err) {
		t.Error("not deleted", err)
	}
	y, m, d := time.Now().Date()
	donefile = filepath.Join(metaDir, doneFileName+fmt.Sprintf(".%d-%d-%d", y, m, d))
	if err = meta.DeleteDoneFile(donefile); err != nil {
		t.Error(err)
	}
	_, err = os.Stat(donefile)
	if err != nil {
		t.Errorf("%v shoud not deleted %v", donefile, err)
	}
}

func Test_getdonefiles(t *testing.T) {
	donefiles := "Test_getdonefiles"
	err := os.Mkdir(donefiles, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(donefiles)
	_, err = os.Create(filepath.Join(donefiles, "file.done.2016-10-01"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	_, err = os.Create(filepath.Join(donefiles, "file.done.2016-10-02"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = os.Create(filepath.Join(donefiles, "file.deleted.2016-10-02"))
	if err != nil {
		t.Fatal(err)
	}
	err = os.Mkdir(filepath.Join(donefiles, "file.done.2016-10-03"), os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	c := conf.MapConf{}
	c[KeyMetaPath] = donefiles
	c[KeyLogPath] = "logpath"
	c[KeyMode] = ModeDir
	c[KeyDataSourceTag] = "tag1path"
	meta, err := NewMetaWithConf(c)
	if err != nil {
		t.Error(err)
	}

	files, err := meta.GetDoneFiles()
	if err != nil {
		t.Error(err)
	}
	exps := []string{"file.done.2016-10-02", "file.done.2016-10-01"}
	var gots []string
	for _, fi := range files {
		gots = append(gots, fi.Info.Name())
	}
	if !reflect.DeepEqual(exps, gots) {
		t.Errorf("Test_getdonefiles error got %v exp %v", gots, exps)
	}
	if meta.GetDataSourceTag() != "tag1path" {
		t.Error("GetDataSourceTag error")
	}
	if meta.GetMode() != ModeDir {
		t.Error("get mode error")
	}
}
