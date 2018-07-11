package file

import (
	"github.com/json-iterator/go"
	"github.com/utahta/go-cronowriter"

	"github.com/qiniu/pandora-go-sdk/base/reqerr"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/utils/models"
)

var _ sender.SkipDeepCopySender = &Sender{}

// Sender write datas into local file
// only for test
type Sender struct {
	name        string
	writer      *cronowriter.CronoWriter
	marshalFunc func([]Data) ([]byte, error)
}

func init() {
	sender.RegisterConstructor(sender.TypeFile, NewSender)
}

// file sender
func NewSender(conf conf.MapConf) (fileSender sender.Sender, err error) {
	var path string
	path, err = conf.GetString(sender.KeyFileSenderPath)
	if err != nil {
		return
	}
	name, _ := conf.GetStringOr(sender.KeyName, "fileSender:"+path)
	fileSender, err = newSender(name, path, JSONLineMarshalFunc)
	if err != nil {
		return
	}
	return
}

func newSender(name, path string, marshalFunc func([]Data) ([]byte, error)) (*Sender, error) {
	f, err := cronowriter.New(path)
	if err != nil {
		return nil, err
	}
	return &Sender{
		name:        name,
		writer:      f,
		marshalFunc: marshalFunc,
	}, nil
}

// Send inherit from Sender
func (fs *Sender) Send(datas []Data) error {
	bytes, err := fs.marshalFunc(datas)
	if err != nil {
		return reqerr.NewSendError(fs.Name()+" Cannot marshal data into file, error is "+err.Error(), sender.ConvertDatasBack(datas), reqerr.TypeDefault)
	}
	_, err = fs.writer.Write(bytes)
	if err != nil {
		return reqerr.NewSendError(fs.Name()+"Cannot write data into file, error is "+err.Error(), sender.ConvertDatasBack(datas), reqerr.TypeDefault)
	}
	return nil
}

func (fs *Sender) Name() string {
	return fs.name
}

func (fs *Sender) Close() error {
	return fs.writer.Close()
}

// JSONLineMarshalFunc  将数据json并且按换行符分隔
func JSONLineMarshalFunc(datas []Data) ([]byte, error) {
	bytes, err := jsoniter.Marshal(datas)
	if err != nil {
		return nil, err
	}
	return append(bytes, '\n'), nil
}

func (_ *Sender) SkipDeepCopy() {}
