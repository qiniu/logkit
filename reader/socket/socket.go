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
	*Reader
}

func (psr *packetSocketReader) listen() {
	buf := make([]byte, 64*1024) // 64kb - maximum size of IP packet

	defer func() {
		if atomic.CompareAndSwapInt32(&psr.status, reader.StatusStopping, reader.StatusStopped) {
			close(psr.ReadChan)
		}
	}()

	for {
		if atomic.LoadInt32(&psr.status) == reader.StatusStopped || atomic.LoadInt32(&psr.status) == reader.StatusStopping {
			return
		}
		n, _, err := psr.PacketConn.ReadFrom(buf)
		if err != nil {
			if !strings.HasSuffix(err.Error(), ": use of closed network connection") {
				log.Error(err)
			}
			break
		}
		// double check
		if atomic.LoadInt32(&psr.status) == reader.StatusStopped || atomic.LoadInt32(&psr.status) == reader.StatusStopping {
			return
		}
		psr.ReadChan <- string(buf[:n])
	}
}

func init() {
	reader.RegisterConstructor(reader.ModeSocket, NewReader)
}

type Reader struct {
	netproto        string
	ServiceAddress  string
	MaxConnections  int
	ReadBufferSize  int
	ReadTimeout     time.Duration
	KeepAlivePeriod time.Duration
	status          int32
	meta            *reader.Meta // 记录offset的元数据

	// resource need  close
	ReadChan chan string
	Closer   io.Closer
}

func (sr *Reader) Name() string {
	return "SocketReader<" + sr.ServiceAddress + ">"
}

func (sr *Reader) Source() string {
	return sr.ServiceAddress
}

func (sr *Reader) SetMode(mode string, v interface{}) error {
	return errors.New("SocketReader not support readmode")
}

func (sr *Reader) SyncMeta() {
	//FIXME 网络监听存在丢包可能性，无法保证不丢包
	return
}

func (sr *Reader) ReadLine() (data string, err error) {
	if atomic.LoadInt32(&sr.status) == reader.StatusInit {
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

func (sr *Reader) Start() error {

	if !atomic.CompareAndSwapInt32(&sr.status, reader.StatusInit, reader.StatusRunning) {
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
			Listener: l,
			Reader:   sr,
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
			PacketConn: pc,
			Reader:     sr,
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

func (sr *Reader) Close() error {
	if atomic.CompareAndSwapInt32(&sr.status, reader.StatusRunning, reader.StatusStopping) {
		log.Infof("Runner[%v] Reader[%v] stopping", sr.meta.RunnerName, sr.Name())
	} else {
		atomic.CompareAndSwapInt32(&sr.status, reader.StatusInit, reader.StatusStopped)
		close(sr.ReadChan)
	}

	var err error
	if sr.Closer != nil {
		err = sr.Closer.Close()
		sr.Closer = nil

		// Make a connection meant to fail but unblock and release the port
		net.Dial(sr.netproto, sr.ServiceAddress)
	}
	log.Infof("Runner[%v] Reader[%v] stopped ", sr.meta.RunnerName, sr.Name())
	return err
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
		ServiceAddress:  ServiceAddress,
		MaxConnections:  MaxConnections,
		ReadBufferSize:  ReadBufferSize,
		ReadTimeout:     ReadTimeoutdur,
		KeepAlivePeriod: KeepAlivePeriodDur,
		ReadChan:        make(chan string),
		status:          reader.StatusInit,
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
