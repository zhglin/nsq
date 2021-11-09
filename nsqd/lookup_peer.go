package nsqd

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/lg"
)

// lookupPeer is a low-level type for connecting/reading/writing to nsqlookupd
//
// A lookupPeer instance is designed to connect lazily to nsqlookupd and reconnect
// gracefully (i.e. it is all handled by the library).  Clients can simply use the
// Command interface to perform a round-trip.
// lookupPeer是用于连接/读/写nsqlookupd的底层类型
// 一个lookupPeer实例被设计为惰性连接到nsqlookupd并优雅地重新连接(也就是说，它全部由库处理)。
// 客户端可以简单地使用Command接口来执行往返。
type lookupPeer struct {
	logf            lg.AppLogFunc
	addr            string   // 服务器地址
	conn            net.Conn // 已建立的tcp链接
	state           int32
	connectCallback func(*lookupPeer) // 建立链接时的回调函数
	maxBodySize     int64             // 最大的报文长度限制
	Info            peerInfo          // identify命令中lookupd返回的信息
}

// peerInfo contains metadata for a lookupPeer instance (and is JSON marshalable)
// 包含lookupPeer实例的元数据(并且是JSON可编组的)
type peerInfo struct {
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
	BroadcastAddress string `json:"broadcast_address"`
}

// newLookupPeer creates a new lookupPeer instance connecting to the supplied address.
//
// The supplied connectCallback will be called *every* time the instance connects.
// 创建一个新的lookupPeer实例连接到提供的地址。
// 每次实例连接时，所提供的connectCallback都会被调用。
func newLookupPeer(addr string, maxBodySize int64, l lg.AppLogFunc, connectCallback func(*lookupPeer)) *lookupPeer {
	return &lookupPeer{
		logf:            l,
		addr:            addr,
		state:           stateDisconnected,
		maxBodySize:     maxBodySize,
		connectCallback: connectCallback,
	}
}

// Connect will Dial the specified address, with timeouts
// 建立tcp链接
func (lp *lookupPeer) Connect() error {
	lp.logf(lg.INFO, "LOOKUP connecting to %s", lp.addr)
	conn, err := net.DialTimeout("tcp", lp.addr, time.Second)
	if err != nil {
		return err
	}
	lp.conn = conn
	return nil
}

// String returns the specified address
func (lp *lookupPeer) String() string {
	return lp.addr
}

// Read implements the io.Reader interface, adding deadlines
func (lp *lookupPeer) Read(data []byte) (int, error) {
	lp.conn.SetReadDeadline(time.Now().Add(time.Second))
	return lp.conn.Read(data)
}

// Write implements the io.Writer interface, adding deadlines
func (lp *lookupPeer) Write(data []byte) (int, error) {
	lp.conn.SetWriteDeadline(time.Now().Add(time.Second))
	return lp.conn.Write(data)
}

// Close implements the io.Closer interface
// 关闭client
func (lp *lookupPeer) Close() error {
	// 设置state状态
	lp.state = stateDisconnected
	// 关闭链接
	if lp.conn != nil {
		return lp.conn.Close()
	}
	return nil
}

// Command performs a round-trip for the specified Command.
//
// It will lazily connect to nsqlookupd and gracefully handle
// reconnecting in the event of a failure.
//
// It returns the response from nsqlookupd as []byte
// 为指定的命令执行往返操作。
// 它将惰性地连接到nsqlookupd，并在发生故障时优雅地处理重新连接。
// 它从nsqlookupd返回响应[]byte
func (lp *lookupPeer) Command(cmd *nsq.Command) ([]byte, error) {
	initialState := lp.state // 最初的状态
	if lp.state != stateConnected {
		// 建立链接
		err := lp.Connect()
		if err != nil {
			return nil, err
		}
		// 设置链接状态
		lp.state = stateConnected
		// 发送协议版本号
		_, err = lp.Write(nsq.MagicV1)
		if err != nil {
			lp.Close()
			return nil, err
		}
		// 初始化状态stateDisconnected，表明重新建立链接了
		if initialState == stateDisconnected {
			lp.connectCallback(lp)
		}
		// connectCallback会改写state
		if lp.state != stateConnected {
			return nil, fmt.Errorf("lookupPeer connectCallback() failed")
		}
	}

	// 完成connect
	if cmd == nil {
		return nil, nil
	}

	// 发送命令
	_, err := cmd.WriteTo(lp)
	if err != nil {
		lp.Close() // 写入异常
		return nil, err
	}

	// 读取响应
	resp, err := readResponseBounded(lp, lp.maxBodySize)
	if err != nil {
		lp.Close() // 读取异常
		return nil, err
	}
	return resp, nil
}

// 读取相应内容
func readResponseBounded(r io.Reader, limit int64) ([]byte, error) {
	var msgSize int32

	// message size 4字节的响应长度
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	// 校验消息长度
	if int64(msgSize) > limit {
		return nil, fmt.Errorf("response body size (%d) is greater than limit (%d)",
			msgSize, limit)
	}

	// message binary data 读取响应内容
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}
