package nsqlookupd

import (
	"io"
	"net"
	"sync"

	"github.com/nsqio/nsq/internal/protocol"
)

// tcp服务端
type tcpServer struct {
	nsqlookupd *NSQLookupd
	conns      sync.Map // 客户端地址对应的Client
}

// Handle 客户端链接的处理函数
func (p *tcpServer) Handle(conn net.Conn) {
	p.nsqlookupd.logf(LOG_INFO, "TCP: new client(%s)", conn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	// 客户端应该通过发送一个4字节的序列来初始化自己，该序列指示它打算通信的协议版本，
	// 这将允许我们优雅地升级协议，从面向文本/行到任何……
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf) //精确地读取len(buf)字节数据填充进buf，不足会返回err
	if err != nil {
		p.nsqlookupd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		conn.Close()
		return
	}
	protocolMagic := string(buf) // 协议版本号

	p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		conn.RemoteAddr(), protocolMagic)

	var prot protocol.Protocol
	switch protocolMagic {
	case "  V1":
		prot = &LookupProtocolV1{nsqlookupd: p.nsqlookupd}
	default:
		// 协议版本号异常 直接关闭
		protocol.SendResponse(conn, []byte("E_BAD_PROTOCOL"))
		conn.Close()
		p.nsqlookupd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			conn.RemoteAddr(), protocolMagic)
		return
	}

	// 创建并保存client
	client := prot.NewClient(conn)
	p.conns.Store(conn.RemoteAddr(), client)

	// for循环解析并处理client报文，一直阻塞，直到异常退出
	err = prot.IOLoop(client)
	if err != nil {
		p.nsqlookupd.logf(LOG_ERROR, "client(%s) - %s", conn.RemoteAddr(), err)
	}

	// 链接异常退出后进行清理并关闭
	p.conns.Delete(conn.RemoteAddr())
	client.Close()
}

// Close 关闭所有客户端链接
func (p *tcpServer) Close() {
	p.conns.Range(func(k, v interface{}) bool {
		v.(protocol.Client).Close()
		return true
	})
}
