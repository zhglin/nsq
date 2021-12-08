package nsqd

import (
	"crypto/md5"
	"crypto/tls"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"

	"github.com/nsqio/nsq/internal/lg"
)

type Options struct {
	// basic options
	ID        int64       `flag:"node-id" cfg:"id"`
	LogLevel  lg.LogLevel `flag:"log-level"`
	LogPrefix string      `flag:"log-prefix"`
	Logger    Logger

	TCPAddress               string        `flag:"tcp-address"`                                                   // tcp服务ip:port
	HTTPAddress              string        `flag:"http-address"`                                                  // http服务ip:port
	HTTPSAddress             string        `flag:"https-address"`                                                 // https服务ip:port
	BroadcastAddress         string        `flag:"broadcast-address"`                                             // 外网ip
	BroadcastTCPPort         int           `flag:"broadcast-tcp-port"`                                            // 外网tcp端口号
	BroadcastHTTPPort        int           `flag:"broadcast-http-port"`                                           // 外网http端口号
	NSQLookupdTCPAddresses   []string      `flag:"lookupd-tcp-address" cfg:"nsqlookupd_tcp_addresses"`            //lookupd地址
	AuthHTTPAddresses        []string      `flag:"auth-http-address" cfg:"auth_http_addresses"`                   // 进行身份校验的请求地址
	HTTPClientConnectTimeout time.Duration `flag:"http-client-connect-timeout" cfg:"http_client_connect_timeout"` // 身份校验建立请求的链接超时时间
	HTTPClientRequestTimeout time.Duration `flag:"http-client-request-timeout" cfg:"http_client_request_timeout"` // 身份校验请求响应的超时时间

	// diskqueue options
	DataPath        string        `flag:"data-path"`          // 存储数据的目录
	MemQueueSize    int64         `flag:"mem-queue-size"`     // 内存中保存的消息数量 topic,channel
	MaxBytesPerFile int64         `flag:"max-bytes-per-file"` // 后端队列的文件最大长度
	SyncEvery       int64         `flag:"sync-every"`
	SyncTimeout     time.Duration `flag:"sync-timeout"`

	// 延迟队列，inFlight队列定时扫描相关配置
	QueueScanInterval        time.Duration // channel扫描时间间隔
	QueueScanRefreshInterval time.Duration // 调整工作协程数的间隔时间
	QueueScanSelectionCount  int           `flag:"queue-scan-selection-count"` // 一个时间间隔扫描的channel数量
	QueueScanWorkerPoolMax   int           `flag:"queue-scan-worker-pool-max"` // 最大开启的工作协程数
	QueueScanDirtyPercent    float64       // 处理成功的百分比，超过就重新随机选channel重试

	// msg and command options
	MsgTimeout    time.Duration `flag:"msg-timeout"`
	MaxMsgTimeout time.Duration `flag:"max-msg-timeout"`
	MaxMsgSize    int64         `flag:"max-msg-size"`    // 一个队列消息的最大长度
	MaxBodySize   int64         `flag:"max-body-size"`   // 支持的最大的报文长度
	MaxReqTimeout time.Duration `flag:"max-req-timeout"` // 延迟消息的最大延迟时间
	ClientTimeout time.Duration // 客户端超时时间  /2 = 心跳检查时间

	// client overridable configuration options
	MaxHeartbeatInterval   time.Duration `flag:"max-heartbeat-interval"` // 最大的心跳检查间隔
	MaxRdyCount            int64         `flag:"max-rdy-count"`          // 最大准备接收的消息量
	MaxOutputBufferSize    int64         `flag:"max-output-buffer-size"`
	MaxOutputBufferTimeout time.Duration `flag:"max-output-buffer-timeout"` // 最大的写缓冲区超时时间
	MinOutputBufferTimeout time.Duration `flag:"min-output-buffer-timeout"` // 最小的写缓冲区超时时间
	OutputBufferTimeout    time.Duration `flag:"output-buffer-timeout"`
	MaxChannelConsumers    int           `flag:"max-channel-consumers"` // 一个channel同时订阅的最大客户端数量

	// statsd integration
	StatsdAddress          string        `flag:"statsd-address"`
	StatsdPrefix           string        `flag:"statsd-prefix"`
	StatsdInterval         time.Duration `flag:"statsd-interval"`
	StatsdMemStats         bool          `flag:"statsd-mem-stats"`
	StatsdUDPPacketSize    int           `flag:"statsd-udp-packet-size"`
	StatsdExcludeEphemeral bool          `flag:"statsd-exclude-ephemeral"`

	// e2e message latency
	E2EProcessingLatencyWindowTime  time.Duration `flag:"e2e-processing-latency-window-time"`
	E2EProcessingLatencyPercentiles []float64     `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles"`

	// TLS config
	TLSCert             string `flag:"tls-cert"`
	TLSKey              string `flag:"tls-key"`
	TLSClientAuthPolicy string `flag:"tls-client-auth-policy"`
	TLSRootCAFile       string `flag:"tls-root-ca-file"`
	TLSRequired         int    `flag:"tls-required"` // 是否开启tls
	TLSMinVersion       uint16 `flag:"tls-min-version"`

	// compression
	DeflateEnabled  bool `flag:"deflate"`
	MaxDeflateLevel int  `flag:"max-deflate-level"`
	SnappyEnabled   bool `flag:"snappy"`
}

// NewOptions 默认的配置选项
func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID:        defaultID,
		LogPrefix: "[nsqd] ",
		LogLevel:  lg.INFO,

		TCPAddress:        "0.0.0.0:4150",
		HTTPAddress:       "0.0.0.0:4151",
		HTTPSAddress:      "0.0.0.0:4152",
		BroadcastAddress:  hostname,
		BroadcastTCPPort:  0,
		BroadcastHTTPPort: 0,

		NSQLookupdTCPAddresses: make([]string, 0),
		AuthHTTPAddresses:      make([]string, 0),

		HTTPClientConnectTimeout: 2 * time.Second,
		HTTPClientRequestTimeout: 5 * time.Second,

		MemQueueSize:    10000,
		MaxBytesPerFile: 100 * 1024 * 1024,
		SyncEvery:       2500,
		SyncTimeout:     2 * time.Second,

		QueueScanInterval:        100 * time.Millisecond,
		QueueScanRefreshInterval: 5 * time.Second,
		QueueScanSelectionCount:  20,
		QueueScanWorkerPoolMax:   4,
		QueueScanDirtyPercent:    0.25,

		MsgTimeout:    60 * time.Second,
		MaxMsgTimeout: 15 * time.Minute,
		MaxMsgSize:    1024 * 1024,
		MaxBodySize:   5 * 1024 * 1024,
		MaxReqTimeout: 1 * time.Hour,
		ClientTimeout: 60 * time.Second,

		MaxHeartbeatInterval:   60 * time.Second,
		MaxRdyCount:            2500,
		MaxOutputBufferSize:    64 * 1024,
		MaxOutputBufferTimeout: 30 * time.Second,
		MinOutputBufferTimeout: 25 * time.Millisecond,
		OutputBufferTimeout:    250 * time.Millisecond,
		MaxChannelConsumers:    0,

		StatsdPrefix:        "nsq.%s",
		StatsdInterval:      60 * time.Second,
		StatsdMemStats:      true,
		StatsdUDPPacketSize: 508,

		E2EProcessingLatencyWindowTime: time.Duration(10 * time.Minute),

		DeflateEnabled:  true,
		MaxDeflateLevel: 6,
		SnappyEnabled:   true,

		TLSMinVersion: tls.VersionTLS10,
	}
}
