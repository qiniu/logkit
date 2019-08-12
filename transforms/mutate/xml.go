package mutate

import (
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/clbanning/mxj"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Xml{}
	_ transforms.Transformer      = &Xml{}
	_ transforms.Initializer      = &Xml{}
)

type Xml struct {
	Key        string `json:"key"`
	New        string `json:"new"`
	Keep       bool   `json:"keep"`
	Expand     bool   `json:"expand"`
	DiscardKey bool   `json:"discard_key"`
	NoAttr     bool   `json:"no_attr"`
	stats      StatsInfo

	keys          []string
	news          []string
	isDelete      bool // 1. 如果keys news相等（Key New相等或者不展开时New为空）, 则删除key value 2. 根据discardKey是否为true进行删除
	cacheNews     map[string][]string
	cacheNewsLock sync.RWMutex
	numRoutine    int
}

func (g *Xml) Init() error {
	var isDelete bool
	if g.Key == g.New {
		isDelete = true // keys news相等
	}
	g.keys = GetKeys(g.Key)
	g.news = GetKeys(g.New)
	if g.New == "" {
		g.news = g.keys
		if g.Expand {
			g.news = g.news[:len(g.news)-1] // keys news不相等
		} else {
			isDelete = true // keys news相等
		}
	}

	if g.DiscardKey || isDelete {
		g.isDelete = true
	}
	g.cacheNews = make(map[string][]string)

	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	g.numRoutine = numRoutine
	return nil
}

func (g *Xml) Transform(datas []Data) ([]Data, error) {
	if g.keys == nil {
		g.Init()
	}
	if g.Expand {
		mxj.LeafUseDotNotation()
		defer mxj.LeafUseDotNotation()
	}

	var (
		dataLen     = len(datas)
		err, fmtErr error
		errNum      int

		numRoutine   = g.numRoutine
		dataPipeline = make(chan transforms.TransformInfo)
		resultChan   = make(chan transforms.TransformResult)
		wg           = new(sync.WaitGroup)
	)

	if dataLen < numRoutine {
		numRoutine = dataLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go g.transform(dataPipeline, resultChan, wg)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	go func() {
		for idx, data := range datas {
			dataPipeline <- transforms.TransformInfo{
				CurData: data,
				Index:   idx,
			}
		}
		close(dataPipeline)
	}()

	var transformResultSlice = make(transforms.TransformResultSlice, dataLen)
	for resultInfo := range resultChan {
		transformResultSlice[resultInfo.Index] = resultInfo
	}

	for _, transformResult := range transformResultSlice {
		if transformResult.Err != nil {
			err = transformResult.Err
			errNum += transformResult.ErrNum
		}
		datas[transformResult.Index] = transformResult.CurData
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(dataLen), g.Type())
	return datas, fmtErr
}

func (g *Xml) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("xml transformer not support rawTransform")
}

func parseXml(xmlStr string, keep bool) (data map[string]interface{}, err error) {
	return mxj.NewMapXml([]byte(xmlStr), !keep)
}

func (g *Xml) Description() string {
	//return "parse xmlString to xml data"
	return "解析xml, 将xml格式转变为json结构"
}

func (g *Xml) Type() string {
	return "xml"
}

func (g *Xml) SampleConfig() string {
	return `{
       "type":"xml",
       "key":"myParseKey",
       "new":"myNewKey",
       "keep": "keepString"
    }`
}

func (g *Xml) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNew,
		transforms.KeyKeepString,
		{
			KeyName:       "discard_key",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{false, true},
			Default:       false,
			DefaultNoUse:  false,
			Description:   "删除原始key指定的字段名和字段值",
			Type:          transforms.TransformTypeBoolean,
			Advance:       true,
		},
		{
			KeyName:       "expand",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{false, true},
			Default:       false,
			DefaultNoUse:  false,
			Description:   "展开xml里的字段（请确保字段名不重名）",
			Type:          transforms.TransformTypeBoolean,
			Advance:       true,
		},
		{
			KeyName:       "no_attr",
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{false, true},
			Default:       false,
			DefaultNoUse:  false,
			Description:   "去除xml里属性相关值",
			Type:          transforms.TransformTypeBoolean,
			Advance:       true,
		},
	}
}

func (g *Xml) Stage() string {
	return transforms.StageAfterParser
}

func (g *Xml) Stats() StatsInfo {
	return g.stats
}

func (g *Xml) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("xml", func() transforms.Transformer {
		return &Xml{}
	})
}

func (g *Xml) transform(dataPipeline <-chan transforms.TransformInfo, resultChan chan transforms.TransformResult, wg *sync.WaitGroup) {
	var (
		err    error
		errNum int
	)
	for transformInfo := range dataPipeline {
		err = nil
		errNum = 0

		val, getErr := GetMapValue(transformInfo.CurData, g.keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, g.Key)
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				ErrNum:  errNum,
				Err:     err,
			}
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			typeErr := errors.New("transform key " + g.Key + " data type is not string")
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				ErrNum:  errNum,
				Err:     err,
			}
			continue
		}
		strVal = strings.TrimSpace(strVal)
		if len(strVal) < 1 {
			continue
		}

		m, xmlErr := mxj.NewMapXml([]byte(strVal), g.Keep)
		if xmlErr != nil {
			errNum, err = transforms.SetError(errNum, xmlErr, transforms.General, "")
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				ErrNum:  errNum,
				Err:     err,
			}
			continue
		}

		errNum, err = g.delete(transformInfo.CurData, errNum, err)

		if g.Expand {
			resultChan <- g.expand(m, transformInfo, errNum)
			continue
		}

		if !g.NoAttr {
			setErr := SetMapValue(transformInfo.CurData, map[string]interface{}(m), false, g.news...)
			if setErr != nil {
				errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.New)
			}
			resultChan <- transforms.TransformResult{
				Index:   transformInfo.Index,
				CurData: transformInfo.CurData,
				ErrNum:  errNum,
				Err:     err,
			}
			continue
		}

		ln := m.LeafNodes(g.NoAttr)
		for _, v := range ln {
			paths := GetKeys(v.Path)
			pathsLen := len(paths)
			if pathsLen == 0 {
				continue
			}
			values := make([]string, len(g.news))
			copy(values, g.news)
			values = append(values, paths...)
			setErr := SetMapValue(transformInfo.CurData, v.Value, false, values...)
			if setErr != nil {
				errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, paths[pathsLen-1])
			}
		}

		resultChan <- transforms.TransformResult{
			Index:   transformInfo.Index,
			CurData: transformInfo.CurData,
			ErrNum:  errNum,
			Err:     err,
		}
	}
	wg.Done()
}

func (g *Xml) delete(curData Data, errNum int, err error) (int, error) {
	if !g.isDelete {
		return errNum, err
	}

	keys := make([]string, len(g.keys))
	copy(keys, g.keys)
	for len(keys) > 0 {
		DeleteMapValue(curData, keys...)
		val, getErr := GetMapValue(curData, keys[:len(keys)-1]...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, strings.Join(keys, "."))
			continue
		}
		if val == nil {
			keys = keys[:len(keys)-1]
			continue
		}
		if valMap, ok := val.(map[string]interface{}); ok && len(valMap) == 0 {
			keys = keys[:len(keys)-1]
			continue
		}
		break
	}
	return errNum, err
}

func (g *Xml) expand(m mxj.Map, transformInfo transforms.TransformInfo, errNum int) transforms.TransformResult {
	var err error
	ln := m.LeafNodes(g.NoAttr)
	for _, v := range ln {
		paths := GetKeys(v.Path)
		pathsLen := len(paths)
		if pathsLen == 0 {
			continue
		}
		currentKey := paths[pathsLen-1]
		var (
			values []string
			ok     bool
		)
		g.cacheNewsLock.RLock()
		values, ok = g.cacheNews[currentKey]
		g.cacheNewsLock.RUnlock()
		if !ok {
			values = make([]string, len(g.news))
			copy(values, g.news)
			values = append(values, currentKey)
			g.cacheNewsLock.Lock()
			g.cacheNews[currentKey] = values
			g.cacheNewsLock.Unlock()
		}

		for num := 1; num < 10; num++ {
			exist, existErr := KeyExist(transformInfo.CurData, v.Value, values...)
			if existErr != nil {
				errNum, err = transforms.SetError(errNum, existErr, transforms.GetErr, paths[pathsLen-1])
				return transforms.TransformResult{
					ErrNum:  errNum,
					Err:     err,
					CurData: transformInfo.CurData,
					Index:   transformInfo.Index,
				}
			}

			if !exist {
				break
			}
			values = make([]string, len(g.news))
			copy(values, g.news)
			values = append(values, currentKey+strconv.Itoa(num))

		}

		setErr := SetMapValue(transformInfo.CurData, v.Value, false, values...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, paths[pathsLen-1])
		}
	}

	return transforms.TransformResult{
		ErrNum:  errNum,
		Err:     err,
		CurData: transformInfo.CurData,
		Index:   transformInfo.Index,
	}
}
