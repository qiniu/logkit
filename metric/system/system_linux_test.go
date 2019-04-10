package system

import "testing"

func Test_getNumDisk(t *testing.T) {
	t.Parallel()
	number := getNumDisk()
	t.Log("get disk number: ", number)
}

func Test_getNumService(t *testing.T) {
	t.Parallel()
	number := getNumService()
	t.Log("get service number: ", number)
}
