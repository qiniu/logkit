package queue

// BackendQueue represents the behavior for the secondary message
// storage system
type BackendQueue interface {
	Name() string
	Put([]byte) error
	ReadChan() <-chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

const (
	FROM_NONE = iota
	FROM_DISK
	FROM_MEMORY
)
