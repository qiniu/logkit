package models

import "errors"

var (
	ErrNotSupport  = errors.New("runner does not support")
	ErrNotExist    = errors.New("runner does not exist")
	ErrQueueClosed = errors.New("queue is closed")
)

const (
	// 一切正常
	ErrNothing = "L200"

	// 单机版 Runner 操作
	ErrConfigName     = "L1001"
	ErrRunnerAdd      = "L1002"
	ErrRunnerDelete   = "L1003"
	ErrRunnerStart    = "L1004"
	ErrRunnerStop     = "L1005"
	ErrRunnerReset    = "L1006"
	ErrRunnerUpdate   = "L1007"
	ErrRunnerErrorGet = "L1008"

	// read 相关
	ErrReadRead = "L1101"
	// parse 相关
	ErrParseParse = "L1201"
	// transform 相关
	ErrTransformTransform = "L1301"
	// send 相关
	ErrSendSend = "L1401"

	// 集群版 master API
	ErrClusterSlaves   = "L2001"
	ErrClusterStatus   = "L2002"
	ErrClusterConfigs  = "L2003"
	ErrClusterRegister = "L2004"
	ErrClusterConfig   = "L2014"

	// 集群版 slave API
	ErrClusterTag = "L2005"

	// 集群版 master 对 slaves 的操作
	ErrClusterRunnerAdd    = "L2006"
	ErrClusterRunnerDelete = "L2007"
	ErrClusterRunnerStart  = "L2008"
	ErrClusterRunnerStop   = "L2009"
	ErrClusterRunnerReset  = "L2010"
	ErrClusterRunnerUpdate = "L2011"
	ErrClusterSlavesDelete = "L2012"
	ErrClusterSlavesTag    = "L2013"
)

var ErrorCodeHumanize = map[string]string{
	ErrNothing: "操作成功",

	ErrConfigName:   "获取 Config 出现错误",
	ErrRunnerAdd:    "添加 Runner 出现错误",
	ErrRunnerDelete: "删除 Runner 出现错误",
	ErrRunnerStart:  "开启 Runner 出现错误",
	ErrRunnerStop:   "关闭 Runner 出现错误",
	ErrRunnerReset:  "重置 Runner 出现错误",
	ErrRunnerUpdate: "更新 Runner 出现错误",

	ErrParseParse: "解析字符串失败",

	ErrTransformTransform: "转化字段失败",

	ErrClusterSlaves:   "获取 Slaves 列表出现错误",
	ErrClusterStatus:   "获取 Slaves 状态出现错误",
	ErrClusterConfig:   "获取 Slaves Config 出现错误",
	ErrClusterConfigs:  "获取 Slaves Configs 出现错误",
	ErrClusterRegister: "接受 Slaves 注册出现错误",

	ErrClusterTag: "更改 Tag 出现错误",

	ErrClusterRunnerAdd:    "Slaves 添加 Runner 出现错误",
	ErrClusterRunnerDelete: "Slaves 删除 Runner 出现错误",
	ErrClusterRunnerStart:  "Slaves 启动 Runner 出现错误",
	ErrClusterRunnerStop:   "Slaves 关闭 Runner 出现错误",
	ErrClusterRunnerReset:  "Slaves 重置 Runner 出现错误",
	ErrClusterRunnerUpdate: "Slaves 更新 Runner 出现错误",
	ErrClusterSlavesDelete: "Slaves 从列表中移除时出现错误",
	ErrClusterSlavesTag:    "Slaves 更改 Tag 出现错误",
}

func IsNotExist(err error) bool {
	return err == ErrNotExist
}

func IsNotSupport(err error) bool {
	return err == ErrNotSupport
}
