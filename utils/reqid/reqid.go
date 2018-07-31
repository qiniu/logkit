package reqid

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"
)

var (
	// In most cases pid is less than math.MaxUint16.
	pid = uint16(os.Getpid())

	ip = uint32(0)

	incIdx = uint32(0)
)

func init() {

	nip, err := pickOne()
	if err == nil {
		ip = nip.Uint32()
	}
}

// -----------------------------------------------------------------------------

type netIPv4 net.IP

func newNetIPv4(ip uint32) netIPv4 {

	b := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(b, ip)
	return netIPv4(net.IP(b))
}

func (ip netIPv4) IsPrivate() bool {

	return ip[0] == 10 ||
		(ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31) ||
		(ip[0] == 192 && ip[1] == 168)
}

func (ip netIPv4) Uint32() uint32 {

	return binary.BigEndian.Uint32(ip)
}

func (ip netIPv4) String() string {

	return net.IP(ip).String()
}

// pickOne pick an global unicast ipv4 from network interfaces,
// it is preferable to pick a public ip.
func pickOne() (ip netIPv4, err error) {

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}
OuterLoop:
	for _, addr := range addrs {
		switch v := addr.(type) {
		case *net.IPNet:
			one := v.IP.To4()
			if one == nil || !one.IsGlobalUnicast() {
				continue
			}
			ip = netIPv4(one)
			if !ip.IsPrivate() {
				break OuterLoop
			}
		}
	}
	if ip == nil {
		err = errors.New("not found")
	}
	return
}

// -----------------------------------------------------------------------------

// ip + pid + time + index
func Gen() string {

	var b [12]byte
	binary.LittleEndian.PutUint32(b[:], ip)
	binary.LittleEndian.PutUint16(b[4:], pid)
	binary.LittleEndian.PutUint32(b[6:], uint32(time.Now().Unix()))
	binary.LittleEndian.PutUint16(b[10:], uint16(atomic.AddUint32(&incIdx, 1)))
	return base64.URLEncoding.EncodeToString(b[:])
}

type Info struct {
	IP    string
	Unix  int64
	Pid   uint32
	Index uint32
}

func Parse(reqId string) (info Info, err error) {

	b, err := base64.URLEncoding.DecodeString(reqId)
	if err != nil {
		return
	}
	if len(b) != 12 {
		err = fmt.Errorf("invalid length %v", b)
		return
	}
	info.IP = newNetIPv4(binary.LittleEndian.Uint32(b[:])).String()
	info.Pid = uint32(binary.LittleEndian.Uint16(b[4:]))
	info.Unix = int64(binary.LittleEndian.Uint32(b[6:]))
	info.Index = uint32(binary.LittleEndian.Uint16(b[10:]))
	return
}
