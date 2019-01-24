package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/axgle/mahonia"
)

const usage = `converter convert files from one specify charset to another 

Usage:

  converter [commands|flags]

The commands & flags are:

  -sc <charset>      source file charset
  -dc <charset>      destination file charset, if you not specify this, default will be UTF-8
  -s  <file>         source file path
  -d  <file>         destination file path
  -l                 list all supported charset
  -t                 test log with all charset

Examples:

  # start logkit
  converter -sc UTF-16 -s /home/test/test.log -d UTF-8 -d /home/test/test.utf-8.log

`

var (
	sc   = flag.String("sc", "", "source file charset")
	dc   = flag.String("dc", "UTF-8", "destination file charset")
	sp   = flag.String("s", "", "source file path")
	dp   = flag.String("d", "decoded.log", "destination file path")
	list = flag.Bool("l", false, "list all supported charset")
	test = flag.Bool("t", false, "test log with all charset")
)

func usageExit(rc int) {
	fmt.Println(usage)
	os.Exit(rc)
}

func main() {
	flag.Usage = func() { usageExit(0) }
	flag.Parse()
	lists := []string{"UTF-8", "UTF-16", "US-ASCII", "ISO-8859-1",
		"GBK", "latin1", "GB18030", "EUC-JP", "UTF-16BE", "UTF-16LE", "Big5", "Shift_JIS",
		"ISO-8859-2", "ISO-8859-3", "ISO-8859-4", "ISO-8859-5", "ISO-8859-6", "ISO-8859-7",
		"ISO-8859-8", "ISO-8859-9", "ISO-8859-10", "ISO-8859-11", "ISO-8859-13",
		"ISO-8859-14", "ISO-8859-15", "ISO-8859-16", "macos-0_2-10.2", "macos-6_2-10.4",
		"macos-7_3-10.2", "macos-29-10.2", "macos-35-10.2", "windows-1250", "windows-1251",
		"windows-1252", "windows-1253", "windows-1254", "windows-1255", "windows-1256",
		"windows-1257", "windows-1258", "windows-874", "IBM037", "ibm-273_P100-1995",
		"ibm-277_P100-1995", "ibm-278_P100-1995", "ibm-280_P100-1995", "ibm-284_P100-1995",
		"ibm-285_P100-1995", "ibm-290_P100-1995", "ibm-297_P100-1995", "ibm-420_X120-1999",
		//此处省略大量IBM的字符集，太多，等用户需要再加
		"KOI8-R", "KOI8-U", "ebcdic-xml-us"}
	if *list {
		fmt.Println("this tool is used to convert files from one specify charset to another, use -h to see how to use it\n all supported charsets are list as belows:")
		for _, v := range lists {
			fmt.Println(v)
		}
		return
	}

	if *sp == "" {
		fmt.Println("you must specify your log path to be converted, use -h to helps")
		return
	}
	datas, err := ioutil.ReadFile(*sp)
	if err != nil {
		fmt.Println("read file ", *sp, " err: ", err)
		return
	}
	if *test {
		if len(datas) > 1024 {
			datas = datas[:1024]
		}
		for _, v := range lists {
			fmt.Println(v)
			decoder := mahonia.NewDecoder(v)
			fmt.Println(decoder.ConvertString(string(datas)))
		}
		return
	}
	if *sc == "" {
		fmt.Println("you must specify your source file charset, use -l to see all supported charsets")
		return
	}
	decoder := mahonia.NewDecoder(*sc)
	ret := decoder.ConvertString(string(datas))
	err = ioutil.WriteFile(*dp, []byte(ret), 0644)
	if err != nil {
		fmt.Println("write file ", *dp, " err: ", err)
	}
	return
}
