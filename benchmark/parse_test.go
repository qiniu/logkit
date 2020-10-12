package benchmark

import (
	"bytes"
	"testing"

	"github.com/go-logfmt/logfmt"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser/config"
	"github.com/qiniu/logkit/parser/csv"
	localLogfmt "github.com/qiniu/logkit/parser/logfmt"
	"github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/logkit/utils/parse/mutate"
)

// cd benchmark
// go test -bench=. -benchtime=3s -run=none
// BenchmarkCSV-4         173257   21040 ns/op csv.Parse()
// BenchmarkLogfmt-4      215204   16546 ns/op logfmt.Parse()
// BenchmarkMutate-4     2276732    1628 ns/op mutate.ScanKeyValue()
// BenchmarkGoLogfmt-4   5140749     729 ns/op go-logfmt.ScanKeyval()
func BenchmarkCSV(b *testing.B) {
	c := conf.MapConf{
		config.KeyCSVSplitter: ",",
		config.KeyCSVSchema:   "name string,age long,mark float",
	}
	p, _ := csv.NewParser(c)
	lines := []string{"david,12,60.2", "david,12,60.2", "david,12,60.2", "david,12,60.2", "david,12,60.2"}
	for i := 0; i < b.N; i++ {
		_, _ = p.Parse(lines)
	}
}

func BenchmarkLogfmt(b *testing.B) {
	p, _ := localLogfmt.NewParser(conf.MapConf{})
	lines := []string{"name=david age=12 mark=60.2", "name=david age=12 mark=60.2", "name=david age=12 mark=60.2",
		"name=david age=12 mark=60.2", "name=david age=12 mark=60.2"}
	for i := 0; i < b.N; i++ {
		_, _ = p.Parse(lines)
	}
}

func BenchmarkMutate(b *testing.B) {
	lines := []string{"name=david age=12 mark=60.2", "name=david age=12 mark=60.2", "name=david age=12 mark=60.2",
		"name=david age=12 mark=60.2", "name=david age=12 mark=60.2"}
	for i := 0; i < b.N; i++ {
		data := models.Data{}
		for _, line := range lines {
			decoder := mutate.NewDecoder(line)
			for decoder.ScanValue("=") {
				data[decoder.Key()] = decoder.Value()
			}
		}
	}
}

func BenchmarkGoLogfmt(b *testing.B) {
	lines := []string{"name=david age=12 mark=60.2", "name=david age=12 mark=60.2", "name=david age=12 mark=60.2",
		"name=david age=12 mark=60.2", "name=david age=12 mark=60.2"}
	for i := 0; i < b.N; i++ {
		data := models.Data{}
		for _, line := range lines {
			reader := bytes.NewReader([]byte(line))
			decoder := logfmt.NewDecoder(reader)
			for decoder.ScanKeyval("="[0]) {
				data[string(decoder.Key())] = string(decoder.Value())
			}
		}
	}
}
