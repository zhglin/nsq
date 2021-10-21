package protocol

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"

	"github.com/nsqio/nsq/internal/lg"
)

type TCPHandler interface {
	Handle(net.Conn)
}

// TCPServer 接收tcp链接 Accept异常直接return
func TCPServer(listener net.Listener, handler TCPHandler, logf lg.AppLogFunc) error {
	logf(lg.INFO, "TCP: listening on %s", listener.Addr())

	var wg sync.WaitGroup

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				// Temporary临时错误
				logf(lg.WARN, "temporary Accept() failure - %s", err)
				runtime.Gosched()
				continue
			}
			// theres no direct way to detect this error because it is not exposed
			// 没有直接的方法检测这个错误，因为它没有暴露出来 链接已关闭
			if !strings.Contains(err.Error(), "use of closed network connection") {
				// 直接return 不wait等待已建立的链接关闭，因为是长链接
				return fmt.Errorf("listener.Accept() error - %s", err)
			}
			break
		}

		wg.Add(1)
		go func() {
			handler.Handle(clientConn) // 处理客户端链接
			wg.Done()
		}()
	}

	// wait to return until all handler goroutines complete
	// 等待返回，直到所有handle处理程序完成
	wg.Wait()

	logf(lg.INFO, "TCP: closing %s", listener.Addr())

	return nil
}
