package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/qiniu/log"
)

const usage = `grok pattern generator generate patterns from specify directory

Usage:

  generators [commands|flags]

The commands & flags are:

  -f  <file>         source file path
  -d  <file>         destination file path

Examples:

  # start logkit
  generator -f /home/logkit/gork_patterns -d /home/logkit/parser/grok/patterns.go

`

var (
	f = flag.String("f", "grok_patterns", "source file path")
	d = flag.String("d", "parser/grok/patterns.go", "destination file path")
)

func usageExit(rc int) {
	fmt.Println(usage)
	os.Exit(rc)
}

// Reads all files in the grok_patterns/ folder
// and encodes them as const DEFAULT_PATTERNS in parser/grok_patterns.go
// remove all comment lines
func main() {
	flag.Usage = func() { usageExit(0) }
	flag.Parse()

	files, err := ioutil.ReadDir(*f)
	if err != nil {
		log.Fatal(err)
	}

	out, _ := os.Create(*d)
	out.Write([]byte("//!!! Notice This is auto generated file, DO NOT EDIT IT!!! \n\npackage grok \n\nconst DEFAULT_PATTERNS = `"))
	for _, file := range files {
		file, _ := os.Open(filepath.Join(*f, file.Name()))
		rd := bufio.NewReader(file)
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
		file.Close()
	}
	out.Write([]byte("`\n"))
	out.Close()
	return
}
