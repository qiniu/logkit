package cronowriter

import "sync"

type nopMutex struct{}

var _ sync.Locker = (*nopMutex)(nil)

func (*nopMutex) Lock()   {}
func (*nopMutex) Unlock() {}
