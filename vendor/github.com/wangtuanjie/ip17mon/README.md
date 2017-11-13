[17mon](http://www.ipip.net/) IP location data for Golang
===

[![Circle CI](https://circleci.com/gh/wangtuanjie/ip17mon.svg?style=svg)](https://circleci.com/gh/wangtuanjie/ip17mon)

## 特性
* 高效的查找算法，查询性能100w/s
* 支持build出的bin文件包含原始数据

## 安装

	go get -u github.com/wangtuanjie/ip17mon


## 使用
	import （
		"fmt"
		"github.com/wangtuanjie/ip17mon"
	）

	func init() {
		if err := ip17mon.Init("your data file"); err != nil {
			panic(err)
		}
	}

	func main() {
		loc, err := ip17mon.Find("116.228.111.18")
		if err != nil {
			fmt.Println("err:", err)
			return
		}
		fmt.Println(loc)
	}

更多请参考[example](https://github.com/wangtuanjie/ip17mon/tree/master/example/qip)



## 许可证

基于 [MIT](https://github.com/wangtuanjie/ip17mon/blob/master/LICENSE) 协议发布

