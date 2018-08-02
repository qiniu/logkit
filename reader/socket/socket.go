package socket

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
	"github.com/qiniu/logkit/reader"
)

var (
	_ reader.DaemonReader = &Reader{}
	_ reader.Reader       = &Reader{}
)

type setReadBufferer interface {
	SetReadBuffer(bytes int) error
}

type streamSocketReader struct {
	Listener net.Listener
	*Reader

	connections    map[string]net.Conn
	connectionsMtx sync.Mutex
}

func (ssr *streamSocketReader) listen() {
	ssr.connections = map[string]net.Conn{}

	defer func() {
		if atomic.CompareAndSwapInt32(&ssr.status, reader.StatusStopping, reader.StatusStopped) {
			close(ssr.readChan)
			close(ssr.errChan)
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

type socketInfo struct {
	address string
	data    string
}

func (ssr *streamSocketReader) read(c net.Conn) {
	defer ssr.removeConnection(c)
	defer c.Close()

	scnr := bufio.NewScanner(c)
	for {
		if atomic.LoadInt32(&ssr.status) == reader.StatusStopped || atomic.LoadInt32(&ssr.status) == reader.StatusStopping {
			return
		}
		if ssr.ReadTimeout != 0 && ssr.ReadTimeout > 0 {
			c.SetReadDeadline(time.Now().Add(ssr.ReadTimeout))
		}
		if !scnr.Scan() {
			break
		}

		//double check
		if atomic.LoadInt32(&ssr.status) == reader.StatusStopped || atomic.LoadInt32(&ssr.status) == reader.StatusStopping {
			return
		}

		var address string
		// get remote addr
		if remoteAddr := c.RemoteAddr(); remoteAddr != nil && len(remoteAddr.String()) != 0 {
			address = remoteAddr.String()
		}
		// if remote addr is empty, get local addr
		if len(address) == 0 {
			if localAddr := c.LocalAddr(); localAddr != nil {
				address = localAddr.String()
			}
		}
		ssr.readChan <- socketInfo{address: address, data: string(scnr.Bytes())}
	}

	if err := scnr.Err(); err != nil {
		if nErr, ok := err.(net.Error); ok && nErr.Timeout() {
			log.Errorf("streamSocketReader Timeout : %s", nErr)
		}
		if !strings.HasSuffix(err.Error(), ": use of closed network connection") {
			log.Error(err)
			//可能reader都已经close了，channel也关了，直接return
			return
		}
		if atomic.LoadInt32(&ssr.status) == reader.StatusStopped || atomic.LoadInt32(&ssr.status) == reader.StatusStopping {
			return
		}
		ssr.sendError(err)
	}
}

type packetSocketReader struct {
	PacketConn net.PacketConn
	*Reader
}

func (psr *packetSocketReader) listen() {
	buf := make([]byte, 64*1024) // 64kb - maximum size of IP packet

	defer func() {
		if atomic.CompareAndSwapInt32(&psr.status, reader.StatusStopping, reader.StatusStopped) {
			close(psr.readChan)
			close(psr.errChan)
		}
	}()

	for {
		if atomic.LoadInt32(&psr.status) == reader.StatusStopped || atomic.LoadInt32(&psr.status) == reader.StatusStopping {
			return
		}
		n, remoteAddr, err := psr.PacketConn.ReadFrom(buf)
		if err != nil {
			if !strings.HasSuffix(err.Error(), ": use of closed network connection") {
				log.Error(err)
			}
			psr.sendError(err)
			break
		}
		// double check
		if atomic.LoadInt32(&psr.status) == reader.StatusStopped || atomic.LoadInt32(&psr.status) == reader.StatusStopping {
			return
		}

		var address string
		// get remote addr
		if remoteAddr != nil && len(remoteAddr.String()) != 0 {
			address = remoteAddr.String()
		}
		// if remote addr is empty, get local addr
		if len(address) == 0 {
			if localAddr := psr.PacketConn.LocalAddr(); localAddr != nil {
				address = localAddr.String()
			}
		}
		val := string(buf[:n])

		psr.readChan <- socketInfo{address: address, data: val}

	}
}

func init() {
	reader.RegisterConstructor(reader.ModeSocket, NewReader)
}

type Reader struct {
	meta *reader.Meta
	// Note: 原子操作，用于表示 reader 整体的运行状态
	status int32

	readChan chan socketInfo
	errChan  chan error

	netproto        string
	ServiceAddress  string
	sourceIp        string
	MaxConnections  int
	ReadBufferSize  int
	ReadTimeout     time.Duration
	KeepAlivePeriod time.Duration

	closer io.Closer
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {
	ServiceAddress, err := conf.GetString(reader.KeySocketServiceAddress)
	if err != nil {
		return nil, err
	}

	MaxConnections, _ := conf.GetIntOr(reader.KeySocketMaxConnections, 0)
	ReadTimeout, _ := conf.GetStringOr(reader.KeySocketReadTimeout, "0")
	ReadTimeoutdur, err := time.ParseDuration(ReadTimeout)
	if err != nil {
		return nil, err
	}
	ReadBufferSize, _ := conf.GetIntOr(reader.KeySocketReadBufferSize, 65535)

	KeepAlivePeriod, _ := conf.GetStringOr(reader.KeySocketKeepAlivePeriod, "5m")
	KeepAlivePeriodDur, err := time.ParseDuration(KeepAlivePeriod)
	if err != nil {
		return nil, err
	}
	return &Reader{
		meta:            meta,
		status:          reader.StatusInit,
		readChan:        make(chan socketInfo),
		errChan:         make(chan error),
		ServiceAddress:  ServiceAddress,
		MaxConnections:  MaxConnections,
		ReadBufferSize:  ReadBufferSize,
		ReadTimeout:     ReadTimeoutdur,
		KeepAlivePeriod: KeepAlivePeriodDur,
	}, nil
}

func (r *Reader) isStopping() bool {
	return atomic.LoadInt32(&r.status) == reader.StatusStopping
}

func (r *Reader) hasStopped() bool {
	return atomic.LoadInt32(&r.status) == reader.StatusStopped
}

func (r *Reader) Name() string {
	return "SocketReader<" + r.ServiceAddress + ">"
}

func (_ *Reader) SetMode(_ string, _ interface{}) error {
	return errors.New("socket reader does not support read mode")
}

func (r *Reader) sendError(err error) {
	if err == nil {
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			log.Errorf("Reader %q was panicked and recovered from %v", r.Name(), rec)
		}
	}()
	r.errChan <- err
}

func (r *Reader) Start() error {
	if r.isStopping() || r.hasStopped() {
		return errors.New("reader is stopping or has stopped")
	} else if !atomic.CompareAndSwapInt32(&r.status, reader.StatusInit, reader.StatusRunning) {
		log.Warnf("Runner[%v] %q daemon has already started and is running", r.meta.RunnerName, r.Name())
		return nil
	}

	spl := strings.SplitN(r.ServiceAddress, "://", 2)
	if len(spl) != 2 {
		return fmt.Errorf("invalid service address: %s", r.ServiceAddress)
	}
	r.netproto = spl[0]
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

		if r.ReadBufferSize > 0 {
			if srb, ok := l.(setReadBufferer); ok {
				srb.SetReadBuffer(r.ReadBufferSize)
			} else {
				log.Warnf("Unable to set read buffer on a %s socket", spl[0])
			}
		}

		ssr := &streamSocketReader{
			Listener: l,
			Reader:   r,
		}

		r.closer = l
		go ssr.listen()
	case "udp", "udp4", "udp6", "ip", "ip4", "ip6", "unixgram":
		pc, err := net.ListenPacket(spl[0], spl[1])
		if err != nil {
			return err
		}
		r.readChan = make(chan socketInfo, 100)

		if r.ReadBufferSize > 0 {
			if srb, ok := pc.(setReadBufferer); ok {
				srb.SetReadBuffer(r.ReadBufferSize)
			} else {
				log.Warnf("Unable to set read buffer on a %s socket", spl[0])
			}
		}

		psr := &packetSocketReader{
			PacketConn: pc,
			Reader:     r,
		}

		r.closer = pc
		go psr.listen()
	default:
		return fmt.Errorf("unknown protocol '%s' in '%s'", spl[0], r.ServiceAddress)
	}

	if spl[0] == "unix" || spl[0] == "unixpacket" || spl[0] == "unixgram" {
		r.closer = unixCloser{path: spl[1], closer: r.closer}
	}

	return nil
}

func (r *Reader) Source() string {
	return r.sourceIp
}

// Note: 对 sourceIp 的操作非线程安全，需由上层逻辑保证同步调用 ReadLine
func (r *Reader) ReadLine() (string, error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case info := <-r.readChan:
		r.sourceIp = info.address
		return info.data, nil
	case <-timer.C:
	}

	return "", nil
}

func (r *Reader) SyncMeta() {
	//FIXME 网络监听存在丢包可能性，无法保证不丢包
}

func (r *Reader) Close() error {
	if !atomic.CompareAndSwapInt32(&r.status, reader.StatusRunning, reader.StatusStopping) {
		log.Warnf("Runner[%v] reader %q is not running, close operation ignored", r.meta.RunnerName, r.Name())
		return nil
	}
	log.Debugf("Runner[%v] %q daemon is stopping", r.meta.RunnerName, r.Name())

	var err error
	if r.closer != nil {
		err = r.closer.Close()
		r.closer = nil

		// Make a connection meant to fail but unblock and release the port
		net.Dial(r.netproto, r.ServiceAddress)
	}
	atomic.StoreInt32(&r.status, reader.StatusStopped)
	log.Infof("Runner[%v] %q daemon has stopped from running", r.meta.RunnerName, r.Name())
	return err
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
