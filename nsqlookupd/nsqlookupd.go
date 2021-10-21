package nsqlookupd

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

type NSQLookupd struct {
	sync.RWMutex
	opts         *Options     // 配置信息
	tcpListener  net.Listener // 接收客户端链接
	httpListener net.Listener
	tcpServer    *tcpServer // 处理客户端链接
	waitGroup    util.WaitGroupWrapper
	DB           *RegistrationDB
}

// New 根据配置信息创建NSQLookupd
func New(opts *Options) (*NSQLookupd, error) {
	var err error

	// 日志
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}
	l := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(),
	}

	l.logf(LOG_INFO, version.String("nsqlookupd"))

	l.tcpServer = &tcpServer{nsqlookupd: l}
	l.tcpListener, err = net.Listen("tcp", opts.TCPAddress) // tcp监听
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	l.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPAddress, err)
	}

	return l, nil
}

// Main starts an instance of nsqlookupd and returns an
// error if there was a problem starting up.
// 启动一个nsqlookupd实例，如果启动时出现问题，则返回一个错误。
func (l *NSQLookupd) Main() error {
	exitCh := make(chan error) // 异常信息

	// 退出 tcp,http有一个异常就发送exitCh信息
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				l.logf(LOG_FATAL, "%s", err)
			}
			exitCh <- err
		})
	}

	// tcp服务
	l.waitGroup.Wrap(func() {
		exitFunc(protocol.TCPServer(l.tcpListener, l.tcpServer, l.logf))
	})

	// http服务
	httpServer := newHTTPServer(l)
	l.waitGroup.Wrap(func() {
		exitFunc(http_api.Serve(l.httpListener, httpServer, "HTTP", l.logf))
	})

	err := <-exitCh
	return err
}

// RealTCPAddr 返回网络地址
func (l *NSQLookupd) RealTCPAddr() *net.TCPAddr {
	return l.tcpListener.Addr().(*net.TCPAddr)
}

// RealHTTPAddr 返回链接的网络地址
func (l *NSQLookupd) RealHTTPAddr() *net.TCPAddr {
	return l.httpListener.Addr().(*net.TCPAddr)
}

// Exit 关闭服务
func (l *NSQLookupd) Exit() {
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	if l.tcpServer != nil {
		l.tcpServer.Close()
	}

	if l.httpListener != nil {
		l.httpListener.Close()
	}
	l.waitGroup.Wait()
}
