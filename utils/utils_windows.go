// +build windows
package utils

import (
	"fmt"
	"os"
	"syscall"
)

func GetIdentifyIDByPath(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return GetIdentifyIDByFile(f)
}

func GetIdentifyIDByFile(f *os.File) (uint64, error) {
	var d syscall.ByHandleFileInformation

	if err := syscall.GetFileInformationByHandle(syscall.Handle(f.Fd()), &d); err != nil {
		err = fmt.Errorf(" syscall.GetFileInformationByHandle error %v", err)
		return 0, err
	}
	inode := uint64(d.FileIndexHigh)
	inode <<= 32
	inode += uint64(d.FileIndexLow)
	return inode, nil
}
