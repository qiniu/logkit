package main

import (
	"bufio"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/qiniu/log"
)

// Reads builtin files in the grok_patterns/ folder
// and encodes them as const DEFAULT_PATTERNS in parser/grok_patterns.go
// remove builtin comment lines
func main() {
	files, err := ioutil.ReadDir("grok_patterns")
	if err != nil {
		log.Fatal(err)
	}
	out, _ := os.Create("parser/grok/patterns.go")
	out.Write([]byte("//!!! Notice This is auto generated file, DO NOT EDIT IT!!! \n\npackage grok \n\nconst DEFAULT_PATTERNS = `"))
	for _, f := range files {
		f, _ := os.Open(filepath.Join("grok_patterns", f.Name()))
		rd := bufio.NewReader(f)
		for {
			data, err := rd.ReadBytes('\n')
			if err != nil {
				if err != io.EOF {
					log.Error("ERROR:", err)
				}
				break
			}
			str := string(data)
			if strings.HasPrefix(str, "#") {
				continue
			}
			out.Write(data)
		}
		f.Close()
	}
	out.Write([]byte("`\n"))
	out.Close()
	return
}
