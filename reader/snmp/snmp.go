package snmp

import (
	"bufio"
	"bytes"
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
	"github.com/qiniu/log"
	"github.com/soniah/gosnmp"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
)

func init() {
	reader.RegisterConstructor(reader.ModeSnmp, NewReader)
}

// Reader holds the configuration for the plugin.
type Reader struct {
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

	Meta     *reader.Meta
	Status   int32
	StopChan chan struct{}
	DataChan chan interface{}
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

func NewReader(meta *reader.Meta, c conf.MapConf) (s reader.Reader, err error) {
	var timeOut, interval time.Duration
	name, _ := c.GetStringOr(reader.KeySnmpReaderName, "logki_default_snmp_name")
	agents, _ := c.GetStringListOr(reader.KeySnmpReaderAgents, []string{"127.0.0.1:161"})
	timeStr, _ := c.GetStringOr(reader.KeySnmpReaderTimeOut, "5s")
	if timeOut, err = time.ParseDuration(timeStr); err != nil {
		return
	}
	intervalStr, _ := c.GetStringOr(reader.KeySnmpReaderInterval, "30s")
	if interval, err = time.ParseDuration(intervalStr); err != nil {
		return
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
		authPassword, _ = c.GetStringOr(reader.KeySnmpReaderAuthPassword, "pass")
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
		return
	}
	if err = jsoniter.Unmarshal([]byte(fieldConf), &fields); err != nil {
		return
	}

	for i := range tables {
		if subErr := tables[i].init(); subErr != nil {
			err = Errorf(subErr, "initializing table %s", tables[i].Name)
			return
		}
	}
	for i := range fields {
		if subErr := fields[i].init(); subErr != nil {
			err = Errorf(subErr, "initializing field %s", fields[i].Name)
			return
		}
	}
	s = &Reader{
		Meta:           meta,
		SnmpName:       name,
		Agents:         agents,
		Timeout:        timeOut,
		Interval:       interval,
		Retries:        retries,
		Version:        uint8(version),
		Community:      community,
		MaxRepetitions: uint8(maxRepetitions),
		ContextName:    contextName,
		SecLevel:       secLevel,
		SecName:        secName,
		AuthProtocol:   authProtocol,
		AuthPassword:   authPassword,
		PrivProtocol:   privProtocol,
		PrivPassword:   privPassword,
		EngineID:       engineID,
		EngineBoots:    uint32(engineBoots),
		EngineTime:     uint32(engineTime),
		Tables:         tables,
		Fields:         fields,

		Status:          reader.StatusInit,
		StopChan:        make(chan struct{}),
		DataChan:        make(chan interface{}, 1000),
		ConnectionCache: make([]snmpConnection, len(agents)),
	}

	return
}

type Table struct {
	Name        string   `json:"table_name"`
	InheritTags []string `json:"table_inherit_tags"`
	IndexAsTag  bool     `json:"table_index_tag"`
	Fields      []Field  `json:"table_fields"`
	Oid         string   `json:"table_oid"`
}

func (t *Table) init() error {
	if err := t.initBuild(); err != nil {
		return err
	}
	for i := range t.Fields {
		if err := t.Fields[i].init(); err != nil {
			return Errorf(err, "initializing field %s", t.Fields[i].Name)
		}
	}
	return nil
}

func (t *Table) initBuild() error {
	if t.Oid == "" {
		return nil
	}

	_, _, oidText, fields, err := snmpTable(t.Oid)
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

func (s *Reader) StoreData(data []map[string]interface{}) (err error) {
	if data == nil || len(data) == 0 {
		return
	}
	for _, d := range data {
		if d == nil || len(d) == 0 {
			continue
		}
		select {
		case <-s.StopChan:
			return
		case s.DataChan <- d:
		}
	}
	return
}

func (s *Reader) Gather() (err error) {
	errMux := new(sync.Mutex)
	var wg sync.WaitGroup
	data := make([]map[string]interface{}, 0)
	for i, agent := range s.Agents {
		wg.Add(1)
		go func(i int, agent string) {
			defer wg.Done()
			gs, err1 := s.getConnection(i)
			if err1 != nil {
				return
			}
			t := Table{
				Name:   s.SnmpName,
				Fields: s.Fields,
			}
			topTags := map[string]string{}
			if data, err1 = s.gatherTable(gs, t, topTags, false); err1 != nil {
				return
			}
			if err1 := s.StoreData(data); err1 != nil {
				errMux.Lock()
				err = err1
				errMux.Unlock()
				return
			}
			for _, t := range s.Tables {
				if data, err1 = s.gatherTable(gs, t, topTags, true); err1 != nil {
					return
				}
				if err1 := s.StoreData(data); err1 != nil {
					errMux.Lock()
					err = err1
					errMux.Unlock()
					return
				}
			}
		}(i, agent)
	}
	wg.Wait()
	return
}

func (s *Reader) gatherTable(gs snmpConnection, t Table, topTags map[string]string, walk bool) (data []map[string]interface{}, err error) {
	var rt *RTable
	rt, err = t.Build(gs, walk)
	if err != nil {
		return
	}
	data = make([]map[string]interface{}, 0)
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
		d := make(map[string]interface{})
		for k, v := range tr.Fields {
			d[k] = v
		}
		for k, v := range tr.Tags {
			d[k] = v
		}
		d[reader.KeyTimestamp] = rt.Time
		d[reader.KeySnmpTableName] = t.Name
		data = append(data, d)
	}
	return

}

//Name reader名称
func (s *Reader) Name() string {
	return s.SnmpName
}

//Source 读取的数据源
func (s *Reader) Source() string {
	return s.SnmpName
}

func (s *Reader) Start() error {
	if !atomic.CompareAndSwapInt32(&s.Status, reader.StatusInit, reader.StatusRunning) {
		return fmt.Errorf("runner[%v] Reader[%v] already started", s.Meta.RunnerName, s.Name())
	}
	go func() {
		ticker := time.NewTicker(s.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := s.Gather()
				if err != nil {
					log.Errorf("runner[%v] Reader[%v] gather error %v", s.Meta.RunnerName, s.Name(), err)
				}
			case <-s.StopChan:
				close(s.DataChan)
				return
			}
		}
	}()
	return nil
}

func (s *Reader) ReadLine() (line string, err error) {
	if atomic.LoadInt32(&s.Status) == reader.StatusInit {
		if err = s.Start(); err != nil {
			log.Error(err)
		}
	}
	select {
	case d := <-s.DataChan:
		var db []byte
		if db, err = jsoniter.Marshal(d); err != nil {
			return
		}
		line = string(db)
	default:
	}
	return
}

func (s *Reader) SetMode(mode string, v interface{}) error {
	return nil
}

func (s *Reader) Close() error {
	close(s.StopChan)
	return nil
}

func (s *Reader) SyncMeta() {
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

func (s *Reader) getConnection(idx int) (snmpConnection, error) {
	if gs := s.ConnectionCache[idx]; gs != nil {
		return gs, nil
	}

	agent := s.Agents[idx]

	gs := gosnmpWrapper{&gosnmp.GoSNMP{}}
	s.ConnectionCache[idx] = gs

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

	gs.Timeout = s.Timeout

	gs.Retries = s.Retries

	switch s.Version {
	case 3:
		gs.Version = gosnmp.Version3
	case 2, 0:
		gs.Version = gosnmp.Version2c
	case 1:
		gs.Version = gosnmp.Version1
	default:
		return nil, fmt.Errorf("invalid version")
	}

	if s.Version < 3 {
		if s.Community == "" {
			gs.Community = "public"
		} else {
			gs.Community = s.Community
		}
	}

	gs.MaxRepetitions = s.MaxRepetitions

	if s.Version == 3 {
		gs.ContextName = s.ContextName

		sp := &gosnmp.UsmSecurityParameters{}
		gs.SecurityParameters = sp
		gs.SecurityModel = gosnmp.UserSecurityModel

		switch strings.ToLower(s.SecLevel) {
		case "noauthnopriv", "":
			gs.MsgFlags = gosnmp.NoAuthNoPriv
		case "authnopriv":
			gs.MsgFlags = gosnmp.AuthNoPriv
		case "authpriv":
			gs.MsgFlags = gosnmp.AuthPriv
		default:
			return nil, fmt.Errorf("invalid secLevel")
		}

		sp.UserName = s.SecName

		switch strings.ToLower(s.AuthProtocol) {
		case "md5":
			sp.AuthenticationProtocol = gosnmp.MD5
		case "sha":
			sp.AuthenticationProtocol = gosnmp.SHA
		case "":
			sp.AuthenticationProtocol = gosnmp.NoAuth
		default:
			return nil, fmt.Errorf("invalid authProtocol")
		}

		sp.AuthenticationPassphrase = s.AuthPassword

		switch strings.ToLower(s.PrivProtocol) {
		case "des":
			sp.PrivacyProtocol = gosnmp.DES
		case "aes":
			sp.PrivacyProtocol = gosnmp.AES
		case "":
			sp.PrivacyProtocol = gosnmp.NoPriv
		default:
			return nil, fmt.Errorf("invalid privProtocol")
		}

		sp.PrivacyPassphrase = s.PrivPassword

		sp.AuthoritativeEngineID = s.EngineID

		sp.AuthoritativeEngineBoots = s.EngineBoots

		sp.AuthoritativeEngineTime = s.EngineTime
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

func snmpTable(oid string) (mibName string, oidNum string, oidText string, fields []Field, err error) {
	snmpTableCachesLock.Lock()
	if snmpTableCaches == nil {
		snmpTableCaches = map[string]snmpTableCache{}
	}

	var stc snmpTableCache
	var ok bool
	if stc, ok = snmpTableCaches[oid]; !ok {
		stc.mibName, stc.oidNum, stc.oidText, stc.fields, stc.err = snmpTableCall(oid)
		snmpTableCaches[oid] = stc
	}

	snmpTableCachesLock.Unlock()
	return stc.mibName, stc.oidNum, stc.oidText, stc.fields, stc.err
}

func snmpTableCall(oid string) (mibName string, oidNum string, oidText string, fields []Field, err error) {
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

	out, err := execCmd("snmptable", "-Ch", "-Cl", "-c", "public", "127.0.0.1", oidFullName)
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
