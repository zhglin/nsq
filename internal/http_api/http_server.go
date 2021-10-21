package http_api

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/nsqio/nsq/internal/lg"
)

type logWriter struct {
	logf lg.AppLogFunc
}

func (l logWriter) Write(p []byte) (int, error) {
	l.logf(lg.WARN, "%s", string(p))
	return len(p), nil
}

// Serve 启动http服务
func Serve(listener net.Listener, handler http.Handler, proto string, logf lg.AppLogFunc) error {
	logf(lg.INFO, "%s: listening on %s", proto, listener.Addr())

	// 开启http
	server := &http.Server{
		Handler:  handler,
		ErrorLog: log.New(logWriter{logf}, "", 0),
	}
	err := server.Serve(listener)
	// theres no direct way to detect this error because it is not exposed
	// 这里没有直接的方法来检测这个错误，因为它没有公开
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		return fmt.Errorf("http.Serve() error - %s", err)
	}

	logf(lg.INFO, "%s: closing %s", proto, listener.Addr())

	return nil
}
