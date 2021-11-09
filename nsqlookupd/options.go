package nsqlookupd

import (
	"log"
	"os"
	"time"

	"github.com/nsqio/nsq/internal/lg"
)

// Options 配置
type Options struct {
	LogLevel  lg.LogLevel `flag:"log-level"`
	LogPrefix string      `flag:"log-prefix"`
	Logger    Logger

	TCPAddress       string `flag:"tcp-address"`       // listen ip地址端口号
	HTTPAddress      string `flag:"http-address"`      // listen http地址端口号
	BroadcastAddress string `flag:"broadcast-address"` // http访问的地址

	InactiveProducerTimeout time.Duration `flag:"inactive-producer-timeout"`
	TombstoneLifetime       time.Duration `flag:"tombstone-lifetime"`
}

// NewOptions 创建默认的配置
func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	return &Options{
		LogPrefix:        "[nsqlookupd] ",
		LogLevel:         lg.INFO,
		TCPAddress:       "0.0.0.0:4160",
		HTTPAddress:      "0.0.0.0:4161",
		BroadcastAddress: hostname, // 主机名

		InactiveProducerTimeout: 300 * time.Second,
		TombstoneLifetime:       45 * time.Second,
	}
}
