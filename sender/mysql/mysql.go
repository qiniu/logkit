package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/logkit/utils/ratelimit"
)

const (
	defaultMySQLPort = "3306"
)

var bufPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

func init() {
	sender.RegisterConstructor(sender.TypeMySQL, NewSender)
}

type dbconn struct {
	datasource string
	table      string

	columns     []string
	queryPrefix string
	placeholder string

	db     *sql.DB
	inited bool
}

func (c *dbconn) init(firstData models.Data) error {
	if c.inited || len(firstData) == 0 {
		return nil
	}

	var err error
	if c.db, err = sql.Open("mysql", c.datasource); err != nil {
		return fmt.Errorf("cannot open DSN %q: %v", c.datasource, err)
	}
	for col := range firstData {
		c.columns = append(c.columns, col)
	}
	sort.Strings(c.columns)

	s := strings.Repeat("?,", len(c.columns))
	c.placeholder = fmt.Sprintf("(%s)", s[:len(s)-1])
	c.queryPrefix = fmt.Sprintf("INSERT INTO %s(%s) VALUES ",
		c.table, strings.Join(c.columns, ","))
	c.inited = true
	return nil
}

func (c *dbconn) write(records []models.Data) error {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	buf.WriteString(c.queryPrefix)
	args := make([]interface{}, 0, len(records)*len(c.columns))
	for _, data := range records {
		buf.WriteString(c.placeholder)
		buf.WriteByte(',')
		for _, col := range c.columns {
			args = append(args, data[col])
		}
	}
	if buf.Len() > len(c.queryPrefix) {
		buf.Truncate(buf.Len() - 1)
	}

	query := buf.String()
	stmt, err := c.db.Prepare(query)
	if err != nil {
		return fmt.Errorf("prepare query %q: %v", query, err)
	}
	defer stmt.Close()

	if _, err = stmt.Exec(args...); err != nil {
		return fmt.Errorf("statement execute: %v", err)
	}
	return nil
}

func (c *dbconn) close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

type Sender struct {
	c    *dbconn
	name string

	limiter ratelimit.Limiter
}

func NewSender(conf conf.MapConf) (s sender.Sender, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch v := r.(type) {
			case error:
				err = v
			default:
				err = fmt.Errorf("%s", v)
			}
		}
	}()

	datasource, err := conf.GetPasswordEnvString(sender.KeyMySQLDataSource)
	if err != nil {
		return nil, err
	}
	table, err := conf.GetString(sender.KeyMySQLTable)
	if err != nil {
		return nil, err
	}
	name, _ := conf.GetStringOr(sender.KeyName, "")
	rate, _ := conf.GetInt64Or(sender.KeyMaxSendRate, -1)

	return &Sender{
		name: name,
		c: &dbconn{
			datasource: datasource,
			table:      table,
		},
		limiter: ratelimit.NewLimiter(rate),
	}, nil
}

func (s *Sender) Send(records []models.Data) error {
	if len(records) == 0 {
		return nil
	}
	if err := s.c.init(records[0]); err != nil {
		return err
	}
	s.limiter.Limit(uint64(len(records)))
	return s.c.write(records)
}

func (s *Sender) Name() string {
	return s.name
}

func (s *Sender) Close() error {
	return s.c.close()
}
