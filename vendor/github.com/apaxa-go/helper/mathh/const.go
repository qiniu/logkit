package mathh

// Untyped constants containing min/max values for (u)int.
// Also define number of bit in int as untyped constant.
// Useful if code can be compiled on architectures with different int size.
// This method should works only for 8, 16, 32, 64 and 128 bits int.
const (
	_max = ^uint(0)                                         // uint max value (constant typed)
	_log = _max>>8&1 + _max>>16&1 + _max>>32&1 + _max>>64&1 // temp

	UintBytes = 1 << _log
	UintBits  = IntBytes * 8
	IntBytes  = UintBytes
	IntBits   = UintBits
	MaxUint   = (1 << IntBits) - 1
	MinUint   = 0
	MaxInt    = MaxUint >> 1
	MinInt    = -MaxInt - 1
)

// Number of bytes and bits in int8 and uint8 and minimal and maximal values for this types.
const (
	Uint8Bytes = 1
	Uint8Bits  = Uint8Bytes * 8
	Int8Bytes  = Uint8Bytes
	Int8Bits   = Uint8Bits
	MaxUint8   = (1 << Uint8Bits) - 1
	MinUint8   = 0
	MaxInt8    = MaxUint8 >> 1
	MinInt8    = -MaxInt8 - 1
)

// Number of bytes and bits in int16 and uint16 and minimal and maximal values for this types.
const (
	Uint16Bytes = 2
	Uint16Bits  = Uint16Bytes * 8
	Int16Bytes  = Uint16Bytes
	Int16Bits   = Uint16Bits
	MaxUint16   = (1 << Uint16Bits) - 1
	MinUint16   = 0
	MaxInt16    = MaxUint16 >> 1
	MinInt16    = -MaxInt16 - 1
)

// Number of bytes and bits in int32 and uint32 and minimal and maximal values for this types.
const (
	Uint32Bytes = 4
	Uint32Bits  = Uint32Bytes * 8
	Int32Bytes  = Uint32Bytes
	Int32Bits   = Uint32Bits
	MaxUint32   = (1 << Uint32Bits) - 1
	MinUint32   = 0
	MaxInt32    = MaxUint32 >> 1
	MinInt32    = -MaxInt32 - 1
)

// Number of bytes and bits in int64 and uint64 and minimal and maximal values for this types.
const (
	Uint64Bytes = 8
	Uint64Bits  = Uint64Bytes * 8
	Int64Bytes  = Uint64Bytes
	Int64Bits   = Uint64Bits
	MaxUint64   = (1 << Uint64Bits) - 1
	MinUint64   = 0
	MaxInt64    = MaxUint64 >> 1
	MinInt64    = -MaxInt64 - 1
)
