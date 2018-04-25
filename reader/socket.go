package reader

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
)

const (

	// 监听的url形式包括：
	// socket_service_address = "tcp://:3110"
	// socket_service_address = "tcp://127.0.0.1:http"
	// socket_service_address = "tcp4://:3110"
	// socket_service_address = "tcp6://:3110"
	// socket_service_address = "tcp6://[2001:db8::1]:3110"
	// socket_service_address = "udp://:3110"
	// socket_service_address = "udp4://:3110"
	// socket_service_address = "udp6://:3110"
	// socket_service_address = "unix:///tmp/sys.sock"
	// socket_service_address = "unixgram:///tmp/sys.sock"
	KeySocketServiceAddress = "socket_service_address"

	// 最大并发连接数
	// 仅用于 stream sockets (e.g. TCP).
	// 0 (default) 为无限制.
	// socket_max_connections = 1024
	KeySocketMaxConnections = "socket_max_connections"

	// 读的超时时间
	// 仅用于 stream sockets (e.g. TCP).
	// 0 (default) 为没有超时
	// socket_read_timeout = "30s"
	KeySocketReadTimeout = "socket_read_timeout"

	// Socket的Buffer大小，默认65535
	// socket_read_buffer_size = 65535
	KeySocketReadBufferSize = "socket_read_buffer_size"

	// TCP连接的keep_alive时长
	// 0 表示关闭keep_alive
	// 默认5分钟
	KeySocketKeepAlivePeriod = "socket_keep_alive_period"
)

type setReadBufferer interface {
	SetReadBuffer(bytes int) error
}

type streamSocketReader struct {
	Listener net.Listener
	*SocketReader

	connections    map[string]net.Conn
	connectionsMtx sync.Mutex
}

func (ssr *streamSocketReader) listen() {
	ssr.connections = map[string]net.Conn{}

	defer func() {
		if atomic.CompareAndSwapInt32(&ssr.status, StatusStopping, StatusStopped) {
			close(ssr.ReadChan)
		}
	}()
	for {
		c, err := ssr.Listener.Accept()
		if err != nil {
			if !strings.HasSuffix(err.Error(), ": use of closed network connection") {
				log.Error(err)
			}
			break
		}

		ssr.connectionsMtx.Lock()
		if ssr.MaxConnections > 0 && len(ssr.connections) >= ssr.MaxConnections {
			ssr.connectionsMtx.Unlock()
			c.Close()
			continue
		}
		ssr.connections[c.RemoteAddr().String()] = c
		ssr.connectionsMtx.Unlock()

		if ssr.netproto == "tcp" || ssr.netproto == "tcp4" || ssr.netproto == "tcp6" {
			if err := ssr.setKeepAlive(c); err != nil {
				log.Error(fmt.Errorf("unable to configure keep alive (%s): %s", ssr.ServiceAddress, err))
			}
		}

		go ssr.read(c)
	}

	ssr.connectionsMtx.Lock()
	for _, c := range ssr.connections {
		c.Close()
	}
	ssr.connectionsMtx.Unlock()
}

func (ssr *streamSocketReader) setKeepAlive(c net.Conn) error {
	tcpc, ok := c.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("cannot set keep alive on a %s socket", strings.SplitN(ssr.ServiceAddress, "://", 2)[0])
	}
	if ssr.KeepAlivePeriod == 0 {
		return tcpc.SetKeepAlive(false)
	}
	if err := tcpc.SetKeepAlive(true); err != nil {
		return err
	}
	return tcpc.SetKeepAlivePeriod(ssr.KeepAlivePeriod)
}

func (ssr *streamSocketReader) removeConnection(c net.Conn) {
	ssr.connectionsMtx.Lock()
	delete(ssr.connections, c.RemoteAddr().String())
	ssr.connectionsMtx.Unlock()
}

func (ssr *streamSocketReader) read(c net.Conn) {
	defer ssr.removeConnection(c)
	defer c.Close()

	scnr := bufio.NewScanner(c)
	for {
		if ssr.ReadTimeout != 0 && ssr.ReadTimeout > 0 {
			c.SetReadDeadline(time.Now().Add(ssr.ReadTimeout))
		}
		if !scnr.Scan() {
			break
		}
		ssr.ReadChan <- string(scnr.Bytes())
	}

	if err := scnr.Err(); err != nil {
		if err, ok := err.(net.Error); ok && err.Timeout() {
			log.Debugf("streamSocketReader Timeout : %s", err)
		} else if !strings.HasSuffix(err.Error(), ": use of closed network connection") {
			log.Error(err)
		}
	}
}

type packetSocketReader struct {
	PacketConn net.PacketConn
	*SocketReader
}

func (psr *packetSocketReader) listen() {
	buf := make([]byte, 64*1024) // 64kb - maximum size of IP packet

	defer func() {
		if atomic.CompareAndSwapInt32(&psr.status, StatusStopping, StatusStopped) {
			close(psr.ReadChan)
		}
	}()

	for {
		n, _, err := psr.PacketConn.ReadFrom(buf)
		if err != nil {
			if !strings.HasSuffix(err.Error(), ": use of closed network connection") {
				log.Error(err)
			}
			break
		}
		psr.ReadChan <- string(buf[:n])
	}
}

type SocketReader struct {
	netproto        string
	ServiceAddress  string
	MaxConnections  int
	ReadBufferSize  int
	ReadTimeout     time.Duration
	KeepAlivePeriod time.Duration
	status          int32
	meta            *Meta // 记录offset的元数据

	// resource need  close
	ReadChan chan string
	Closer   io.Closer
}

func (sr *SocketReader) Name() string {
	return "SocketReader<" + sr.ServiceAddress + ">"
}

func (sr *SocketReader) Source() string {
	return sr.ServiceAddress
}

func (sr *SocketReader) SetMode(mode string, v interface{}) error {
	return errors.New("SocketReader not support readmode")
}

func (sr *SocketReader) SyncMeta() {
	//TODO 网络监听存在丢包可能性，无法保证不丢包
	return
}

func (sr *SocketReader) ReadLine() (data string, err error) {
	if atomic.LoadInt32(&sr.status) == StatusInit {
		err = sr.Start()
		if err != nil {
			log.Error(err)
		}
	}
	timer := time.NewTimer(time.Second)
	select {
	case dat := <-sr.ReadChan:
		data = string(dat)
	case <-timer.C:
	}
	timer.Stop()
	return
}

func (sr *SocketReader) Start() error {

	if !atomic.CompareAndSwapInt32(&sr.status, StatusInit, StatusRunning) {
		return errors.New("socketReader aleady started")
	}

	spl := strings.SplitN(sr.ServiceAddress, "://", 2)
	if len(spl) != 2 {
		return fmt.Errorf("invalid service address: %s", sr.ServiceAddress)
	}
	sr.netproto = spl[0]
	if spl[0] == "unix" || spl[0] == "unixpacket" || spl[0] == "unixgram" {
		// 通过remove来检测套接字文件是否存在
		os.Remove(spl[1])
	}

	switch spl[0] {
	case "tcp", "tcp4", "tcp6", "unix", "unixpacket":
		l, err := net.Listen(spl[0], spl[1])
		if err != nil {
			return err
		}

		if sr.ReadBufferSize > 0 {
			if srb, ok := l.(setReadBufferer); ok {
				srb.SetReadBuffer(sr.ReadBufferSize)
			} else {
				log.Warnf("Unable to set read buffer on a %s socket", spl[0])
			}
		}

		ssr := &streamSocketReader{
			Listener:     l,
			SocketReader: sr,
		}

		sr.Closer = l
		go ssr.listen()
	case "udp", "udp4", "udp6", "ip", "ip4", "ip6", "unixgram":
		pc, err := net.ListenPacket(spl[0], spl[1])
		if err != nil {
			return err
		}

		if sr.ReadBufferSize > 0 {
			if srb, ok := pc.(setReadBufferer); ok {
				srb.SetReadBuffer(sr.ReadBufferSize)
			} else {
				log.Warnf("Unable to set read buffer on a %s socket", spl[0])
			}
		}

		psr := &packetSocketReader{
			PacketConn:   pc,
			SocketReader: sr,
		}

		sr.Closer = pc
		go psr.listen()
	default:
		return fmt.Errorf("unknown protocol '%s' in '%s'", spl[0], sr.ServiceAddress)
	}

	if spl[0] == "unix" || spl[0] == "unixpacket" || spl[0] == "unixgram" {
		sr.Closer = unixCloser{path: spl[1], closer: sr.Closer}
	}

	return nil
}

func (sr *SocketReader) Close() (err error) {
	if atomic.CompareAndSwapInt32(&sr.status, StatusRunning, StatusStopping) {
		log.Infof("Runner[%v] Reader[%v] stopping", sr.meta.RunnerName, sr.Name())
	} else {
		close(sr.ReadChan)
	}

	if sr.Closer != nil {
		err = sr.Closer.Close()
		sr.Closer = nil
	}
	log.Infof("Runner[%v] Reader[%v] stopped ", sr.meta.RunnerName, sr.Name())
	return
}

func NewSocketReader(meta *Meta, conf conf.MapConf) (Reader, error) {
	ServiceAddress, err := conf.GetString(KeySocketServiceAddress)
	if err != nil {
		return nil, err
	}

	MaxConnections, _ := conf.GetIntOr(KeySocketMaxConnections, 0)
	ReadTimeout, _ := conf.GetStringOr(KeySocketReadTimeout, "0")
	ReadTimeoutdur, err := time.ParseDuration(ReadTimeout)
	if err != nil {
		return nil, err
	}
	ReadBufferSize, _ := conf.GetIntOr(KeySocketReadBufferSize, 65535)

	KeepAlivePeriod, _ := conf.GetStringOr(KeySocketKeepAlivePeriod, "5m")
	KeepAlivePeriodDur, err := time.ParseDuration(KeepAlivePeriod)
	if err != nil {
		return nil, err
	}
	return &SocketReader{
		ServiceAddress:  ServiceAddress,
		MaxConnections:  MaxConnections,
		ReadBufferSize:  ReadBufferSize,
		ReadTimeout:     ReadTimeoutdur,
		KeepAlivePeriod: KeepAlivePeriodDur,
		ReadChan:        make(chan string),
		status:          StatusInit,
		meta:            meta,
	}, nil
}

type unixCloser struct {
	path   string
	closer io.Closer
}

func (uc unixCloser) Close() error {
	err := uc.closer.Close()
	os.Remove(uc.path) // ignore error
	return err
}
