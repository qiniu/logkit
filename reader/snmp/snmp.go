package snmp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/json-iterator/go"
	"github.com/soniah/gosnmp"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ reader.DaemonReader = &Reader{}
	_ reader.DataReader   = &Reader{}
	_ reader.Reader       = &Reader{}
)

func init() {
	reader.RegisterConstructor(reader.ModeSnmp, NewReader)
}

type readInfo struct {
	data  Data
	bytes int64
}

// Reader holds the configuration for the plugin.
type Reader struct {
	meta *reader.Meta
	// Note: 原子操作，用于表示 reader 整体的运行状态
	status int32
	/*
		Note: 原子操作，用于表示获取数据的线程运行状态

		- StatusInit: 当前没有任务在执行
		- StatusRunning: 当前有任务正在执行
		- StatusStopping: 数据管道已经由上层关闭，执行中的任务完成时直接退出无需再处理
	*/
	routineStatus int32

	stopChan chan struct{}
	readChan chan readInfo
	errChan  chan error

	SnmpName       string
	Agents         []string      // agent address [ip:port]
	Timeout        time.Duration // 等待回复的时间
	Interval       time.Duration // 收集频率
	Retries        int
	Version        uint8  // 1， 2， 3
	Community      string // version 1&2 的参数
	MaxRepetitions uint8  // version 2&3 的参数
	ContextName    string // version 3 参数

	SecLevel     string //"noAuthNoPriv", "authNoPriv", "authPriv"
	SecName      string
	AuthProtocol string // "MD5", "SHA", "", 默认: ""
	AuthPassword string
	PrivProtocol string // "DES", "AES", "", 默认: ""
	PrivPassword string
	EngineID     string
	EngineBoots  uint32
	EngineTime   uint32

	Tables          []Table
	Fields          []Field
	ConnectionCache []snmpConnection
}

var execCommand = exec.Command

func execCmd(arg0 string, args ...string) ([]byte, error) {
	out, err := execCommand(arg0, args...).Output()
	if err != nil {
		if err, ok := err.(*exec.ExitError); ok {
			return nil, NestedError{
				Err:       err,
				NestedErr: fmt.Errorf("%s", bytes.TrimRight(err.Stderr, "\r\n")),
			}
		}
		return nil, err
	}
	return out, nil
}

func NewReader(meta *reader.Meta, c conf.MapConf) (reader.Reader, error) {
	name, _ := c.GetStringOr(reader.KeySnmpReaderName, "logkit_default_snmp_name")
	agents, _ := c.GetStringListOr(reader.KeySnmpReaderAgents, []string{"127.0.0.1:161"})
	tableHost, _ := c.GetStringOr(reader.KeySnmpTableInitHost, "127.0.0.1")
	timeStr, _ := c.GetStringOr(reader.KeySnmpReaderTimeOut, "5s")
	timeOut, err := time.ParseDuration(timeStr)
	if err != nil {
		return nil, err
	}
	intervalStr, _ := c.GetStringOr(reader.KeySnmpReaderInterval, "30s")
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		return nil, err
	}
	retries, _ := c.GetIntOr(reader.KeySnmpReaderRetries, 3)
	version, _ := c.GetIntOr(reader.KeySnmpReaderVersion, 2)
	var maxRepetitions, engineBoots, engineTime int
	var community, contextName, secLevel, secName, authProtocol, authPassword, privProtocol, privPassword, engineID string
	if version == 1 || version == 2 {
		community, _ = c.GetStringOr(reader.KeySnmpReaderCommunity, "public")
	}
	if version == 2 || version == 3 {
		maxRepetitions, _ = c.GetIntOr(reader.KeySnmpReaderMaxRepetitions, 50)
	}
	if version == 3 {
		contextName, _ = c.GetStringOr(reader.KeySnmpReaderContextName, "")
		secLevel, _ = c.GetStringOr(reader.KeySnmpReaderSecLevel, "noAuthNoPriv")
		secName, _ = c.GetStringOr(reader.KeySnmpReaderSecName, "user")
		authProtocol, _ = c.GetStringOr(reader.KeySnmpReaderAuthProtocol, "MD5")
		authPassword, _ = c.GetPasswordEnvStringOr(reader.KeySnmpReaderAuthPassword, "")
		privProtocol, _ = c.GetStringOr(reader.KeySnmpReaderPrivProtocol, "DES")
		privPassword, _ = c.GetStringOr(reader.KeySnmpReaderPrivPassword, "mypass")
		engineID, _ = c.GetString(reader.KeySnmpReaderEngineID)
		engineBoots, _ = c.GetInt(reader.KeySnmpReaderEngineBoots)
		engineTime, _ = c.GetInt(reader.KeySnmpReaderEngineTime)
	}
	tableConf, _ := c.GetStringOr(reader.KeySnmpReaderTables, "[]")
	fieldConf, _ := c.GetStringOr(reader.KeySnmpReaderFields, "[]")

	var tables []Table
	var fields []Field

	if err = jsoniter.Unmarshal([]byte(tableConf), &tables); err != nil {
		return nil, err
	}
	if err = jsoniter.Unmarshal([]byte(fieldConf), &fields); err != nil {
		return nil, err
	}

	for i := range tables {
		if subErr := tables[i].init(tableHost); subErr != nil {
			return nil, Errorf(subErr, "initializing table %s", tables[i].Name)
		}
	}
	for i := range fields {
		if subErr := fields[i].init(); subErr != nil {
			return nil, Errorf(subErr, "initializing field %s", fields[i].Name)
		}
	}
	return &Reader{
		meta:            meta,
		status:          reader.StatusInit,
		routineStatus:   reader.StatusInit,
		stopChan:        make(chan struct{}),
		readChan:        make(chan readInfo, 1000),
		errChan:         make(chan error),
		SnmpName:        name,
		Agents:          agents,
		Timeout:         timeOut,
		Interval:        interval,
		Retries:         retries,
		Version:         uint8(version),
		Community:       community,
		MaxRepetitions:  uint8(maxRepetitions),
		ContextName:     contextName,
		SecLevel:        secLevel,
		SecName:         secName,
		AuthProtocol:    authProtocol,
		AuthPassword:    authPassword,
		PrivProtocol:    privProtocol,
		PrivPassword:    privPassword,
		EngineID:        engineID,
		EngineBoots:     uint32(engineBoots),
		EngineTime:      uint32(engineTime),
		Tables:          tables,
		Fields:          fields,
		ConnectionCache: make([]snmpConnection, len(agents)),
	}, nil
}

func (r *Reader) isStopping() bool {
	return atomic.LoadInt32(&r.status) == reader.StatusStopping
}

func (r *Reader) hasStopped() bool {
	return atomic.LoadInt32(&r.status) == reader.StatusStopped
}

func (r *Reader) Name() string {
	return r.SnmpName
}

func (r *Reader) SetMode(mode string, v interface{}) error {
	return nil
}

func (r *Reader) sendError(err error) {
	if err == nil {
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			log.Errorf("Reader %q was panicked and recovered from %v", r.Name(), rec)
		}
	}()
	r.errChan <- err
}

func (r *Reader) Start() error {
	if r.isStopping() || r.hasStopped() {
		return errors.New("reader is stopping or has stopped")
	} else if !atomic.CompareAndSwapInt32(&r.status, reader.StatusInit, reader.StatusRunning) {
		log.Warnf("Runner[%v] %q daemon has already started and is running", r.meta.RunnerName, r.Name())
		return nil
	}

	go func() {
		ticker := time.NewTicker(r.Interval)
		defer ticker.Stop()
		for {
			err := r.Gather()
			if err != nil {
				log.Errorf("Runner[%v] %q gather failed: %v ", r.meta.RunnerName, r.Name(), err)
				log.Error(err)
				r.sendError(err)
			}

			select {
			case <-r.stopChan:
				atomic.StoreInt32(&r.status, reader.StatusStopped)
				log.Infof("Runner[%v] %q daemon has stopped from running", r.meta.RunnerName, r.Name())
				return
			case <-ticker.C:
			}
		}
	}()
	log.Infof("Runner[%v] %q daemon has started", r.meta.RunnerName, r.Name())
	return nil
}

func (r *Reader) Source() string {
	return r.SnmpName
}

func (r *Reader) ReadLine() (string, error) {
	return "", errors.New("method ReadLine is not supported, please use ReadData")
}

func (r *Reader) ReadData() (Data, int64, error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case info := <-r.readChan:
		return info.data, info.bytes, nil
	case err := <-r.errChan:
		return nil, 0, err
	case <-timer.C:
	}

	return nil, 0, nil
}

func (r *Reader) SyncMeta() {}

func (r *Reader) Close() error {
	if !atomic.CompareAndSwapInt32(&r.status, reader.StatusRunning, reader.StatusStopping) {
		log.Warnf("Runner[%v] reader %q is not running, close operation ignored", r.meta.RunnerName, r.Name())
		return nil
	}
	log.Debugf("Runner[%v] %q daemon is stopping", r.meta.RunnerName, r.Name())
	close(r.stopChan)

	// 如果此时没有 routine 正在运行，则在此处关闭数据管道，否则由 routine 在退出时负责关闭
	if atomic.CompareAndSwapInt32(&r.routineStatus, reader.StatusInit, reader.StatusStopping) {
		close(r.readChan)
		close(r.errChan)
	}
	return nil
}

type Table struct {
	Name        string   `json:"table_name"`
	InheritTags []string `json:"table_inherit_tags"`
	IndexAsTag  bool     `json:"table_index_tag"`
	Fields      []Field  `json:"table_fields"`
	Oid         string   `json:"table_oid"`
}

func (t *Table) init(host string) error {
	if err := t.initBuild(host); err != nil {
		return err
	}
	for i := range t.Fields {
		if err := t.Fields[i].init(); err != nil {
			return Errorf(err, "initializing field %s", t.Fields[i].Name)
		}
	}
	return nil
}

func (t *Table) initBuild(host string) error {
	if t.Oid == "" {
		return nil
	}

	_, _, oidText, fields, err := snmpTable(t.Oid, host)
	if err != nil {
		return err
	}
	if t.Name == "" {
		t.Name = oidText
	}
	t.Fields = append(t.Fields, fields...)

	return nil
}

type Field struct {
	Name           string `json:"field_name"`
	Oid            string `json:"field_oid"`
	OidIndexSuffix string `json:"field_oid_index_suffix"`
	IsTag          bool   `json:"field_is_tag"`
	Conversion     string `json:"field_conversion"`
}

func (f *Field) init() error {
	_, oidNum, oidText, conversion, err := snmpTranslate(f.Oid)
	if err != nil {
		return Errorf(err, "translating")
	}
	f.Oid = oidNum
	if f.Name == "" {
		f.Name = oidText
	}
	if f.Conversion == "" {
		f.Conversion = conversion
	}

	//TODO use textual convention conversion from the MIB

	return nil
}

type RTable struct {
	Name string
	Time string
	Rows []RTableRow
}

type RTableRow struct {
	Tags   map[string]string
	Fields map[string]interface{}
}

type NestedError struct {
	Err       error
	NestedErr error
}

func (ne NestedError) Error() string {
	return ne.Err.Error() + ": " + ne.NestedErr.Error()
}

func Errorf(err error, msg string, format ...interface{}) error {
	return NestedError{
		NestedErr: err,
		Err:       fmt.Errorf(msg, format...),
	}
}

func (r *Reader) StoreData(datas []Data) (err error) {
	if datas == nil || len(datas) == 0 {
		return
	}
	for _, data := range datas {
		if data == nil || len(data) == 0 {
			continue
		}
		select {
		case <-r.stopChan:
			return
		case r.readChan <- readInfo{data, int64(len(fmt.Sprintf("%s", data)))}:
		}
	}
	return
}

func (r *Reader) Gather() error {
	// 未在准备状态（StatusInit）时无法执行此次任务
	if !atomic.CompareAndSwapInt32(&r.routineStatus, reader.StatusInit, reader.StatusRunning) {
		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %q daemon has stopped, this task does not need to be executed and is skipped this time", r.meta.RunnerName, r.Name())
		} else {
			log.Errorf("Runner[%v] %q daemon is still working on last task, this task will not be executed and is skipped this time", r.meta.RunnerName, r.Name())
		}
		return nil
	}
	defer func() {
		// 如果 reader 在 routine 运行时关闭，则需要此 routine 负责关闭数据管道
		if r.isStopping() || r.hasStopped() {
			if atomic.CompareAndSwapInt32(&r.routineStatus, reader.StatusRunning, reader.StatusStopping) {
				close(r.readChan)
				close(r.errChan)
			}
			return
		}
		atomic.StoreInt32(&r.routineStatus, reader.StatusInit)
	}()

	var wg sync.WaitGroup
	datas := make([]Data, 0)
	errChan := make(chan error, len(r.Agents))
	for i, agent := range r.Agents {
		wg.Add(1)
		go func(i int, agent string) {
			log.Debugf("Runner[%v] %q is reading from agent: %s", r.meta.RunnerName, r.Name(), agent)
			defer wg.Done()

			conn, err := r.getConnection(i)
			if err != nil {
				errChan <- err
				return
			}
			t := Table{
				Name:   r.SnmpName,
				Fields: r.Fields,
			}
			topTags := map[string]string{}
			if datas, err = r.gatherTable(conn, t, topTags, false); err != nil {
				errChan <- err
				return
			}
			if err = r.StoreData(datas); err != nil {
				errChan <- err
				return
			}
			for _, t := range r.Tables {
				if datas, err = r.gatherTable(conn, t, topTags, true); err != nil {
					errChan <- err
					return
				}
				if err = r.StoreData(datas); err != nil {
					errChan <- err
					return
				}
			}
		}(i, agent)
	}
	wg.Wait()
	close(errChan)
	var lastErr error
	for err := range errChan {
		log.Errorf("gather error: %v", err)
		lastErr = err
	}
	return lastErr
}

func (r *Reader) gatherTable(gs snmpConnection, t Table, topTags map[string]string, walk bool) ([]Data, error) {
	rt, err := t.Build(gs, walk)
	if err != nil {
		return nil, err
	}
	datas := make([]Data, 0, len(rt.Rows))
	for _, tr := range rt.Rows {
		if !walk {
			for k, v := range tr.Tags {
				topTags[k] = v
			}
		} else {
			for _, k := range t.InheritTags {
				if v, ok := topTags[k]; ok {
					tr.Tags[k] = v
				}
			}
		}
		if _, ok := tr.Tags["agent_host"]; !ok {
			tr.Tags["agent_host"] = gs.Host()
		}

		data := make(Data)
		for k, v := range tr.Fields {
			data[k] = v
		}
		for k, v := range tr.Tags {
			data[k] = v
		}
		data[reader.KeyTimestamp] = rt.Time
		data[reader.KeySnmpTableName] = t.Name
		datas = append(datas, data)
	}
	return datas, nil
}

func (t Table) Build(gs snmpConnection, walk bool) (*RTable, error) {
	rows := map[string]RTableRow{}

	tagCount := 0
	for _, f := range t.Fields {
		if f.IsTag {
			tagCount++
		}

		if len(f.Oid) == 0 {
			return nil, fmt.Errorf("cannot have empty OID on field %s", f.Name)
		}
		var oid string
		if f.Oid[0] == '.' {
			oid = f.Oid
		} else {
			oid = "." + f.Oid
		}

		ifv := map[string]interface{}{}

		if !walk {
			if pkt, err := gs.Get([]string{oid}); err != nil {
				return nil, Errorf(err, "performing get on field %s", f.Name)
			} else if pkt != nil && len(pkt.Variables) > 0 && pkt.Variables[0].Type != gosnmp.NoSuchObject && pkt.Variables[0].Type != gosnmp.NoSuchInstance {
				ent := pkt.Variables[0]
				fv, err := fieldConvert(f.Conversion, ent.Value)
				if err != nil {
					return nil, Errorf(err, "converting %q (OID %s) for field %s", ent.Value, ent.Name, f.Name)
				}
				ifv[""] = fv
			}
		} else {
			err := gs.Walk(oid, func(ent gosnmp.SnmpPDU) error {
				if len(ent.Name) <= len(oid) || ent.Name[:len(oid)+1] != oid+"." {
					return NestedError{} // break the walk
				}
				idx := ent.Name[len(oid):]
				if f.OidIndexSuffix != "" {
					if !strings.HasSuffix(idx, f.OidIndexSuffix) {
						return nil
					}
					idx = idx[:len(idx)-len(f.OidIndexSuffix)]
				}
				fv, err := fieldConvert(f.Conversion, ent.Value)
				if err != nil {
					return Errorf(err, "converting %q (OID %s) for field %s", ent.Value, ent.Name, f.Name)
				}
				ifv[idx] = fv
				return nil
			})
			if err != nil {
				if _, ok := err.(NestedError); !ok {
					return nil, Errorf(err, "performing bulk walk for field %s", f.Name)
				}
			}
		}

		for idx, v := range ifv {
			rtr, ok := rows[idx]
			if !ok {
				rtr = RTableRow{}
				rtr.Tags = map[string]string{}
				rtr.Fields = map[string]interface{}{}
				rows[idx] = rtr
			}
			if t.IndexAsTag && idx != "" {
				if idx[0] == '.' {
					idx = idx[1:]
				}
				rtr.Tags["index"] = idx
			}
			if vs, ok := v.(string); !ok || vs != "" {
				if f.IsTag {
					if ok {
						rtr.Tags[f.Name] = vs
					} else {
						rtr.Tags[f.Name] = fmt.Sprintf("%v", v)
					}
				} else {
					rtr.Fields[f.Name] = v
				}
			}
		}
	}

	rt := RTable{
		Name: t.Name,
		Rows: make([]RTableRow, 0, len(rows)),
		Time: time.Now().Format(time.RFC3339Nano),
	}
	for _, r := range rows {
		rt.Rows = append(rt.Rows, r)
	}
	return &rt, nil
}

type snmpConnection interface {
	Host() string
	Walk(string, gosnmp.WalkFunc) error
	Get(oids []string) (*gosnmp.SnmpPacket, error)
}

type gosnmpWrapper struct {
	*gosnmp.GoSNMP
}

func (gsw gosnmpWrapper) Host() string {
	return gsw.Target
}

func (gsw gosnmpWrapper) Walk(oid string, fn gosnmp.WalkFunc) error {
	var err error
	// On error, retry once.
	// Unfortunately we can't distinguish between an error returned by gosnmp, and one returned by the walk function.
	for i := 0; i < 2; i++ {
		if gsw.Version == gosnmp.Version1 {
			err = gsw.GoSNMP.Walk(oid, fn)
		} else {
			err = gsw.GoSNMP.BulkWalk(oid, fn)
		}
		if err == nil {
			return nil
		}
		if err := gsw.GoSNMP.Connect(); err != nil {
			return Errorf(err, "reconnecting")
		}
	}
	return err
}

func (gsw gosnmpWrapper) Get(oids []string) (*gosnmp.SnmpPacket, error) {
	var err error
	var pkt *gosnmp.SnmpPacket
	for i := 0; i < 2; i++ {
		pkt, err = gsw.GoSNMP.Get(oids)
		if err == nil {
			return pkt, nil
		}
		if err := gsw.GoSNMP.Connect(); err != nil {
			return nil, Errorf(err, "reconnecting")
		}
	}
	return nil, err
}

func (r *Reader) getConnection(idx int) (snmpConnection, error) {
	if gs := r.ConnectionCache[idx]; gs != nil {
		return gs, nil
	}

	agent := r.Agents[idx]

	gs := gosnmpWrapper{&gosnmp.GoSNMP{}}
	r.ConnectionCache[idx] = gs

	host, portStr, err := net.SplitHostPort(agent)
	if err != nil {
		if err, ok := err.(*net.AddrError); !ok || err.Err != "missing port in address" {
			return nil, Errorf(err, "parsing host")
		}
		host = agent
		portStr = "161"
	}
	gs.Target = host

	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, Errorf(err, "parsing port")
	}
	gs.Port = uint16(port)

	gs.Timeout = r.Timeout

	gs.Retries = r.Retries

	switch r.Version {
	case 3:
		gs.Version = gosnmp.Version3
	case 2, 0:
		gs.Version = gosnmp.Version2c
	case 1:
		gs.Version = gosnmp.Version1
	default:
		return nil, fmt.Errorf("invalid version")
	}

	if r.Version < 3 {
		if r.Community == "" {
			gs.Community = "public"
		} else {
			gs.Community = r.Community
		}
	}

	gs.MaxRepetitions = r.MaxRepetitions

	if r.Version == 3 {
		gs.ContextName = r.ContextName

		sp := &gosnmp.UsmSecurityParameters{}
		gs.SecurityParameters = sp
		gs.SecurityModel = gosnmp.UserSecurityModel

		switch strings.ToLower(r.SecLevel) {
		case "noauthnopriv", "":
			gs.MsgFlags = gosnmp.NoAuthNoPriv
		case "authnopriv":
			gs.MsgFlags = gosnmp.AuthNoPriv
		case "authpriv":
			gs.MsgFlags = gosnmp.AuthPriv
		default:
			return nil, fmt.Errorf("invalid secLevel")
		}

		sp.UserName = r.SecName

		switch strings.ToLower(r.AuthProtocol) {
		case "md5":
			sp.AuthenticationProtocol = gosnmp.MD5
		case "sha":
			sp.AuthenticationProtocol = gosnmp.SHA
		case "":
			sp.AuthenticationProtocol = gosnmp.NoAuth
		default:
			return nil, fmt.Errorf("invalid authProtocol")
		}

		sp.AuthenticationPassphrase = r.AuthPassword

		switch strings.ToLower(r.PrivProtocol) {
		case "des":
			sp.PrivacyProtocol = gosnmp.DES
		case "aes":
			sp.PrivacyProtocol = gosnmp.AES
		case "":
			sp.PrivacyProtocol = gosnmp.NoPriv
		default:
			return nil, fmt.Errorf("invalid privProtocol")
		}

		sp.PrivacyPassphrase = r.PrivPassword

		sp.AuthoritativeEngineID = r.EngineID

		sp.AuthoritativeEngineBoots = r.EngineBoots

		sp.AuthoritativeEngineTime = r.EngineTime
	}

	if err := gs.Connect(); err != nil {
		return nil, Errorf(err, "setting up connection")
	}

	return gs, nil
}

func fieldConvert(conv string, v interface{}) (interface{}, error) {
	if conv == "" {
		if bs, ok := v.([]byte); ok {
			return string(bs), nil
		}
		return v, nil
	}

	var d int
	if _, err := fmt.Sscanf(conv, "float(%d)", &d); err == nil || conv == "float" {
		switch vt := v.(type) {
		case float32:
			v = float64(vt) / math.Pow10(d)
		case float64:
			v = float64(vt) / math.Pow10(d)
		case int:
			v = float64(vt) / math.Pow10(d)
		case int8:
			v = float64(vt) / math.Pow10(d)
		case int16:
			v = float64(vt) / math.Pow10(d)
		case int32:
			v = float64(vt) / math.Pow10(d)
		case int64:
			v = float64(vt) / math.Pow10(d)
		case uint:
			v = float64(vt) / math.Pow10(d)
		case uint8:
			v = float64(vt) / math.Pow10(d)
		case uint16:
			v = float64(vt) / math.Pow10(d)
		case uint32:
			v = float64(vt) / math.Pow10(d)
		case uint64:
			v = float64(vt) / math.Pow10(d)
		case []byte:
			vf, _ := strconv.ParseFloat(string(vt), 64)
			v = vf / math.Pow10(d)
		case string:
			vf, _ := strconv.ParseFloat(vt, 64)
			v = vf / math.Pow10(d)
		}
		return v, nil
	}

	if conv == "int" {
		switch vt := v.(type) {
		case float32:
			v = int64(vt)
		case float64:
			v = int64(vt)
		case int:
			v = int64(vt)
		case int8:
			v = int64(vt)
		case int16:
			v = int64(vt)
		case int32:
			v = int64(vt)
		case int64:
			v = int64(vt)
		case uint:
			v = int64(vt)
		case uint8:
			v = int64(vt)
		case uint16:
			v = int64(vt)
		case uint32:
			v = int64(vt)
		case uint64:
			v = int64(vt)
		case []byte:
			v, _ = strconv.Atoi(string(vt))
		case string:
			v, _ = strconv.Atoi(vt)
		}
		return v, nil
	}

	if conv == "hwaddr" {
		switch vt := v.(type) {
		case string:
			v = net.HardwareAddr(vt).String()
		case []byte:
			v = net.HardwareAddr(vt).String()
		default:
			return nil, fmt.Errorf("invalid type (%T) for hwaddr conversion", v)
		}
		return v, nil
	}

	if conv == "ipaddr" {
		var ipbs []byte

		switch vt := v.(type) {
		case string:
			ipbs = []byte(vt)
		case []byte:
			ipbs = vt
		default:
			return nil, fmt.Errorf("invalid type (%T) for ipaddr conversion", v)
		}

		switch len(ipbs) {
		case 4, 16:
			v = net.IP(ipbs).String()
		default:
			return nil, fmt.Errorf("invalid length (%d) for ipaddr conversion", len(ipbs))
		}

		return v, nil
	}

	return nil, fmt.Errorf("invalid conversion type '%s'", conv)
}

type snmpTableCache struct {
	mibName string
	oidNum  string
	oidText string
	fields  []Field
	err     error
}

var snmpTableCaches map[string]snmpTableCache
var snmpTableCachesLock sync.Mutex

func snmpTable(oid, host string) (mibName string, oidNum string, oidText string, fields []Field, err error) {
	snmpTableCachesLock.Lock()
	if snmpTableCaches == nil {
		snmpTableCaches = map[string]snmpTableCache{}
	}

	var stc snmpTableCache
	var ok bool
	if stc, ok = snmpTableCaches[oid]; !ok {
		stc.mibName, stc.oidNum, stc.oidText, stc.fields, stc.err = snmpTableCall(oid, host)
		snmpTableCaches[oid] = stc
	}

	snmpTableCachesLock.Unlock()
	return stc.mibName, stc.oidNum, stc.oidText, stc.fields, stc.err
}

func snmpTableCall(oid, host string) (mibName string, oidNum string, oidText string, fields []Field, err error) {
	mibName, oidNum, oidText, _, err = snmpTranslate(oid)
	if err != nil {
		return "", "", "", nil, Errorf(err, "translating")
	}

	mibPrefix := mibName + "::"
	oidFullName := mibPrefix + oidText

	tagOids := map[string]struct{}{}
	if out, err := execCmd("snmptranslate", "-Td", oidFullName+".1"); err == nil {
		scanner := bufio.NewScanner(bytes.NewBuffer(out))
		for scanner.Scan() {
			line := scanner.Text()

			if !strings.HasPrefix(line, "  INDEX") {
				continue
			}

			i := strings.Index(line, "{ ")
			if i == -1 { // parse error
				continue
			}
			line = line[i+2:]
			i = strings.Index(line, " }")
			if i == -1 { // parse error
				continue
			}
			line = line[:i]
			for _, col := range strings.Split(line, ", ") {
				tagOids[mibPrefix+col] = struct{}{}
			}
		}
	}

	out, err := execCmd("snmptable", "-Ch", "-Cl", "-c", "public", host, oidFullName)
	if err != nil {
		return "", "", "", nil, Errorf(err, "getting table columns")
	}
	scanner := bufio.NewScanner(bytes.NewBuffer(out))
	scanner.Scan()
	cols := scanner.Text()
	if len(cols) == 0 {
		return "", "", "", nil, fmt.Errorf("could not find any columns in table")
	}
	for _, col := range strings.Split(cols, " ") {
		if len(col) == 0 {
			continue
		}
		_, isTag := tagOids[mibPrefix+col]
		fields = append(fields, Field{Name: col, Oid: mibPrefix + col, IsTag: isTag})
	}

	return mibName, oidNum, oidText, fields, err
}

type snmpTranslateCache struct {
	mibName    string
	oidNum     string
	oidText    string
	conversion string
	err        error
}

var snmpTranslateCachesLock sync.Mutex
var snmpTranslateCaches map[string]snmpTranslateCache

func snmpTranslate(oid string) (mibName string, oidNum string, oidText string, conversion string, err error) {
	snmpTranslateCachesLock.Lock()
	if snmpTranslateCaches == nil {
		snmpTranslateCaches = map[string]snmpTranslateCache{}
	}

	var stc snmpTranslateCache
	var ok bool
	if stc, ok = snmpTranslateCaches[oid]; !ok {

		stc.mibName, stc.oidNum, stc.oidText, stc.conversion, stc.err = snmpTranslateCall(oid)
		snmpTranslateCaches[oid] = stc
	}

	snmpTranslateCachesLock.Unlock()

	return stc.mibName, stc.oidNum, stc.oidText, stc.conversion, stc.err
}

func snmpTranslateCall(oid string) (mibName string, oidNum string, oidText string, conversion string, err error) {
	var out []byte
	if strings.ContainsAny(oid, ":abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") {
		out, err = execCmd("snmptranslate", "-Td", "-Ob", oid)
	} else {
		out, err = execCmd("snmptranslate", "-Td", "-Ob", "-m", "all", oid)
		if err, ok := err.(*exec.Error); ok && err.Err == exec.ErrNotFound {
			return "", oid, oid, "", nil
		}
	}
	if err != nil {
		return "", "", "", "", err
	}

	scanner := bufio.NewScanner(bytes.NewBuffer(out))
	ok := scanner.Scan()
	if !ok && scanner.Err() != nil {
		return "", "", "", "", Errorf(scanner.Err(), "getting OID text")
	}

	oidText = scanner.Text()

	i := strings.Index(oidText, "::")
	if i == -1 {
		if bytes.Contains(out, []byte("[TRUNCATED]")) {
			return "", oid, oid, "", nil
		}
		oidText = oid
	} else {
		mibName = oidText[:i]
		oidText = oidText[i+2:]
	}

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "  -- TEXTUAL CONVENTION ") {
			tc := strings.TrimPrefix(line, "  -- TEXTUAL CONVENTION ")
			switch tc {
			case "MacAddress", "PhysAddress":
				conversion = "hwaddr"
			case "InetAddressIPv4", "InetAddressIPv6", "InetAddress":
				conversion = "ipaddr"
			}
		} else if strings.HasPrefix(line, "::= { ") {
			objs := strings.TrimPrefix(line, "::= { ")
			objs = strings.TrimSuffix(objs, " }")

			for _, obj := range strings.Split(objs, " ") {
				if len(obj) == 0 {
					continue
				}
				if i := strings.Index(obj, "("); i != -1 {
					obj = obj[i+1:]
					oidNum += "." + obj[:strings.Index(obj, ")")]
				} else {
					oidNum += "." + obj
				}
			}
			break
		}
	}
	return mibName, oidNum, oidText, conversion, nil
}
