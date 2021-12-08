package nsqd

import (
	"bufio"
	"compress/flate"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/nsqio/nsq/internal/auth"
)

const defaultBufferSize = 16 * 1024 // 缓冲区大小

// 此客户端链接的状态
const (
	stateInit         = iota // 已初始化
	stateDisconnected        // 已关闭链接 lookupd
	stateConnected           // 已建立链接 lookupd
	stateSubscribed          // 已订阅
	stateClosing             // 已关闭
)

// 客户端标识信息
type identifyDataV2 struct {
	ClientID            string `json:"client_id"`
	Hostname            string `json:"hostname"`
	HeartbeatInterval   int    `json:"heartbeat_interval"`
	OutputBufferSize    int    `json:"output_buffer_size"`
	OutputBufferTimeout int    `json:"output_buffer_timeout"`
	FeatureNegotiation  bool   `json:"feature_negotiation"`
	TLSv1               bool   `json:"tls_v1"`
	Deflate             bool   `json:"deflate"`
	DeflateLevel        int    `json:"deflate_level"`
	Snappy              bool   `json:"snappy"`
	SampleRate          int32  `json:"sample_rate"`
	UserAgent           string `json:"user_agent"`
	MsgTimeout          int    `json:"msg_timeout"`
}

type identifyEvent struct {
	OutputBufferTimeout time.Duration
	HeartbeatInterval   time.Duration
	SampleRate          int32
	MsgTimeout          time.Duration
}

type PubCount struct {
	Topic string `json:"topic"`
	Count uint64 `json:"count"`
}

type ClientV2Stats struct {
	ClientID        string `json:"client_id"`
	Hostname        string `json:"hostname"`
	Version         string `json:"version"`
	RemoteAddress   string `json:"remote_address"`
	State           int32  `json:"state"`
	ReadyCount      int64  `json:"ready_count"`
	InFlightCount   int64  `json:"in_flight_count"`
	MessageCount    uint64 `json:"message_count"`
	FinishCount     uint64 `json:"finish_count"`
	RequeueCount    uint64 `json:"requeue_count"`
	ConnectTime     int64  `json:"connect_ts"`
	SampleRate      int32  `json:"sample_rate"`
	Deflate         bool   `json:"deflate"`
	Snappy          bool   `json:"snappy"`
	UserAgent       string `json:"user_agent"`
	Authed          bool   `json:"authed,omitempty"`
	AuthIdentity    string `json:"auth_identity,omitempty"`
	AuthIdentityURL string `json:"auth_identity_url,omitempty"`

	PubCounts []PubCount `json:"pub_counts,omitempty"`

	TLS                           bool   `json:"tls"`
	CipherSuite                   string `json:"tls_cipher_suite"`
	TLSVersion                    string `json:"tls_version"`
	TLSNegotiatedProtocol         string `json:"tls_negotiated_protocol"`
	TLSNegotiatedProtocolIsMutual bool   `json:"tls_negotiated_protocol_is_mutual"`
}

func (s ClientV2Stats) String() string {
	connectTime := time.Unix(s.ConnectTime, 0)
	duration := time.Since(connectTime).Truncate(time.Second)

	_, port, _ := net.SplitHostPort(s.RemoteAddress)
	id := fmt.Sprintf("%s:%s %s", s.Hostname, port, s.UserAgent)

	// producer
	if len(s.PubCounts) > 0 {
		var total uint64
		var topicOut []string
		for _, v := range s.PubCounts {
			total += v.Count
			topicOut = append(topicOut, fmt.Sprintf("%s=%d", v.Topic, v.Count))
		}
		return fmt.Sprintf("[%s %-21s] msgs: %-8d topics: %s connected: %s",
			s.Version,
			id,
			total,
			strings.Join(topicOut, ","),
			duration,
		)
	}

	// consumer
	return fmt.Sprintf("[%s %-21s] state: %d inflt: %-4d rdy: %-4d fin: %-8d re-q: %-8d msgs: %-8d connected: %s",
		s.Version,
		id,
		s.State,
		s.InFlightCount,
		s.ReadyCount,
		s.FinishCount,
		s.RequeueCount,
		s.MessageCount,
		duration,
	)
}

// 客户端链接
type clientV2 struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	// 64位原子变量需要首先在32位平台上正确对齐
	ReadyCount    int64  // 准备接收的消息数量，inFlight消息超过此值暂停
	InFlightCount int64  // 飞行中消息数量
	MessageCount  uint64 // 发送的消息数量
	FinishCount   uint64 // FIN完成的消息数
	RequeueCount  uint64

	pubCounts map[string]uint64 // 数据统计 topicName=>写入的消息数

	writeLock sync.RWMutex
	metaLock  sync.RWMutex

	ID        int64 // 自增序列号
	nsqd      *NSQD
	UserAgent string // Identify报文

	// original connection
	net.Conn // tcp原始连接

	// connections based on negotiated features
	tlsConn     *tls.Conn
	flateWriter *flate.Writer // 支持压缩的链接

	// reading/writing interfaces
	Reader *bufio.Reader // 读缓冲
	Writer *bufio.Writer // 写缓冲

	OutputBufferSize    int           // 全局默认，Identify报文可改, 写缓冲区大小
	OutputBufferTimeout time.Duration // 全局默认，Identify报文可改, 刷新写缓冲区的时间间隔

	HeartbeatInterval time.Duration // 全局默认，Identify报文可改，心跳检查间隔时间

	MsgTimeout time.Duration // 全局默认，Identify报文可改，消息超时时间

	State          int32     // 链接状态
	ConnectTime    time.Time // 接收到链接的当前时间
	Channel        *Channel  // 订阅的channel
	ReadyStateChan chan int  // 准备接收消息数量的变更通知
	ExitChan       chan int

	ClientID string // 客户端标识 ip identify报文
	Hostname string // 客户端标识 ip	Identify报文

	SampleRate int32 // 全局默认，Identify报文可改，采样率 随机丢弃消息

	IdentifyEventChan chan identifyEvent // 客户端identify报文上传的信息
	SubEventChan      chan *Channel      // 已订阅的channel

	TLS     int32 // 是否是TLS链接
	Snappy  int32 // 是否进行Snappy报文压缩 1是
	Deflate int32 // 是否进行Deflate报文压缩 1是

	// re-usable buffer for reading the 4-byte lengths off the wire
	// 4字节的报文长度
	lenBuf   [4]byte
	lenSlice []byte // lenSlice = lenBuf[:]

	AuthSecret string      // 认证报文
	AuthState  *auth.State // 身份认证结果
}

// 创建客户端链接
func newClientV2(id int64, conn net.Conn, nsqd *NSQD) *clientV2 {
	// 客户端的标识 host
	var identifier string
	if conn != nil {
		identifier, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
	}

	c := &clientV2{
		ID:   id,
		nsqd: nsqd,

		Conn: conn,

		Reader: bufio.NewReaderSize(conn, defaultBufferSize), // 最少有size尺寸的缓冲
		Writer: bufio.NewWriterSize(conn, defaultBufferSize), // 最少有size尺寸的缓冲

		OutputBufferSize:    defaultBufferSize,
		OutputBufferTimeout: nsqd.getOpts().OutputBufferTimeout,

		MsgTimeout: nsqd.getOpts().MsgTimeout,

		// ReadyStateChan has a buffer of 1 to guarantee that in the event
		// there is a race the state update is not lost
		// ReadyStateChan有一个1的缓冲区，以保证在发生竞赛时状态更新不会丢失
		ReadyStateChan: make(chan int, 1),
		ExitChan:       make(chan int),
		ConnectTime:    time.Now(), // 链接时间
		State:          stateInit,  // 链接状态

		ClientID: identifier,
		Hostname: identifier,

		SubEventChan:      make(chan *Channel, 1),
		IdentifyEventChan: make(chan identifyEvent, 1),

		// heartbeats are client configurable but default to 30s
		// 心跳是客户端可配置的，但默认为30秒
		HeartbeatInterval: nsqd.getOpts().ClientTimeout / 2,

		pubCounts: make(map[string]uint64),
	}
	c.lenSlice = c.lenBuf[:]
	return c
}

// 客户端地址
func (c *clientV2) String() string {
	return c.RemoteAddr().String()
}

// Type 客户端类型
func (c *clientV2) Type() int {
	c.metaLock.RLock()
	hasPublished := len(c.pubCounts) > 0
	c.metaLock.RUnlock()
	if hasPublished {
		return typeProducer
	}
	return typeConsumer
}

// Identify 客户端上报的客户端信息
func (c *clientV2) Identify(data identifyDataV2) error {
	c.nsqd.logf(LOG_INFO, "[%s] IDENTIFY: %+v", c, data)

	c.metaLock.Lock()
	c.ClientID = data.ClientID
	c.Hostname = data.Hostname
	c.UserAgent = data.UserAgent
	c.metaLock.Unlock()

	err := c.SetHeartbeatInterval(data.HeartbeatInterval)
	if err != nil {
		return err
	}

	err = c.SetOutputBuffer(data.OutputBufferSize, data.OutputBufferTimeout)
	if err != nil {
		return err
	}

	err = c.SetSampleRate(data.SampleRate)
	if err != nil {
		return err
	}

	err = c.SetMsgTimeout(data.MsgTimeout)
	if err != nil {
		return err
	}

	ie := identifyEvent{
		OutputBufferTimeout: c.OutputBufferTimeout,
		HeartbeatInterval:   c.HeartbeatInterval,
		SampleRate:          c.SampleRate,
		MsgTimeout:          c.MsgTimeout,
	}

	// update the client's message pump
	select {
	case c.IdentifyEventChan <- ie:
	default:
	}

	return nil
}

// Stats 客户端链接状态
func (c *clientV2) Stats(topicName string) ClientStats {
	c.metaLock.RLock()
	clientID := c.ClientID
	hostname := c.Hostname
	userAgent := c.UserAgent
	var identity string
	var identityURL string
	if c.AuthState != nil {
		identity = c.AuthState.Identity
		identityURL = c.AuthState.IdentityURL
	}
	pubCounts := make([]PubCount, 0, len(c.pubCounts))
	for topic, count := range c.pubCounts {
		if len(topicName) > 0 && topic != topicName {
			continue
		}
		pubCounts = append(pubCounts, PubCount{
			Topic: topic,
			Count: count,
		})
		break
	}
	c.metaLock.RUnlock()
	stats := ClientV2Stats{
		Version:         "V2",
		RemoteAddress:   c.RemoteAddr().String(),
		ClientID:        clientID,
		Hostname:        hostname,
		UserAgent:       userAgent,
		State:           atomic.LoadInt32(&c.State),
		ReadyCount:      atomic.LoadInt64(&c.ReadyCount),
		InFlightCount:   atomic.LoadInt64(&c.InFlightCount),
		MessageCount:    atomic.LoadUint64(&c.MessageCount),
		FinishCount:     atomic.LoadUint64(&c.FinishCount),
		RequeueCount:    atomic.LoadUint64(&c.RequeueCount),
		ConnectTime:     c.ConnectTime.Unix(),
		SampleRate:      atomic.LoadInt32(&c.SampleRate),
		TLS:             atomic.LoadInt32(&c.TLS) == 1,
		Deflate:         atomic.LoadInt32(&c.Deflate) == 1,
		Snappy:          atomic.LoadInt32(&c.Snappy) == 1,
		Authed:          c.HasAuthorizations(),
		AuthIdentity:    identity,
		AuthIdentityURL: identityURL,
		PubCounts:       pubCounts,
	}
	if stats.TLS {
		p := prettyConnectionState{c.tlsConn.ConnectionState()}
		stats.CipherSuite = p.GetCipherSuite()
		stats.TLSVersion = p.GetVersion()
		stats.TLSNegotiatedProtocol = p.NegotiatedProtocol
		stats.TLSNegotiatedProtocolIsMutual = p.NegotiatedProtocolIsMutual
	}
	return stats
}

// struct to convert from integers to the human readable strings
type prettyConnectionState struct {
	tls.ConnectionState
}

func (p *prettyConnectionState) GetCipherSuite() string {
	switch p.CipherSuite {
	case tls.TLS_RSA_WITH_RC4_128_SHA:
		return "TLS_RSA_WITH_RC4_128_SHA"
	case tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_RSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"
	}
	return fmt.Sprintf("Unknown %d", p.CipherSuite)
}

func (p *prettyConnectionState) GetVersion() string {
	switch p.Version {
	case tls.VersionSSL30:
		return "SSL30"
	case tls.VersionTLS10:
		return "TLS1.0"
	case tls.VersionTLS11:
		return "TLS1.1"
	case tls.VersionTLS12:
		return "TLS1.2"
	default:
		return fmt.Sprintf("Unknown %d", p.Version)
	}
}

// IsReadyForMessages 客户端是否准备就绪 接收消息
func (c *clientV2) IsReadyForMessages() bool {
	// channel是否已暂停
	if c.Channel.IsPaused() {
		return false
	}

	readyCount := atomic.LoadInt64(&c.ReadyCount) // RDY请求
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)

	c.nsqd.logf(LOG_DEBUG, "[%s] state rdy: %4d inflt: %4d", c, readyCount, inFlightCount)

	// inFlight消息的校验
	if inFlightCount >= readyCount || readyCount <= 0 {
		return false
	}

	return true
}

// SetReadyCount 准备接收的消息数量
func (c *clientV2) SetReadyCount(count int64) {
	oldCount := atomic.SwapInt64(&c.ReadyCount, count)

	if oldCount != count {
		c.tryUpdateReadyState()
	}
}

// 写入变更
func (c *clientV2) tryUpdateReadyState() {
	// you can always *try* to write to ReadyStateChan because in the cases
	// where you cannot the message pump loop would have iterated anyway.
	// the atomic integer operations guarantee correctness of the value.
	// 您总是可以*尝试*写入ReadyStateChan，因为在您不能执行消息泵循环的情况下，无论如何都会迭代。原子整型操作保证值的正确性。
	select {
	case c.ReadyStateChan <- 1:
	default:
	}
}

// FinishedMessage 统计计数
func (c *clientV2) FinishedMessage() {
	atomic.AddUint64(&c.FinishCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

// Empty channel关闭时的回调
func (c *clientV2) Empty() {
	atomic.StoreInt64(&c.InFlightCount, 0)
	c.tryUpdateReadyState()
}

// SendingMessage 统计计数
func (c *clientV2) SendingMessage() {
	atomic.AddInt64(&c.InFlightCount, 1)
	atomic.AddUint64(&c.MessageCount, 1)
}

// PublishedMessage 记录写入的消息数量
func (c *clientV2) PublishedMessage(topic string, count uint64) {
	c.metaLock.Lock()
	c.pubCounts[topic] += count
	c.metaLock.Unlock()
}

// TimedOutMessage 超时消息回调
func (c *clientV2) TimedOutMessage() {
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *clientV2) RequeuedMessage() {
	atomic.AddUint64(&c.RequeueCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

// StartClose 关闭client
func (c *clientV2) StartClose() {
	// Force the client into ready 0 修改客户端准备就绪的消息数
	c.SetReadyCount(0)
	// mark this client as closing 将该客户端标记为关闭
	atomic.StoreInt32(&c.State, stateClosing)
}

// Pause channel暂停的回调
func (c *clientV2) Pause() {
	c.tryUpdateReadyState()
}

// UnPause channel取消暂停的回调
func (c *clientV2) UnPause() {
	c.tryUpdateReadyState()
}

// SetHeartbeatInterval 设置HeartbeatInterval心跳检查
func (c *clientV2) SetHeartbeatInterval(desiredInterval int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case desiredInterval == -1:
		c.HeartbeatInterval = 0
	case desiredInterval == 0:
		// do nothing (use default)
	case desiredInterval >= 1000 &&
		desiredInterval <= int(c.nsqd.getOpts().MaxHeartbeatInterval/time.Millisecond):
		c.HeartbeatInterval = time.Duration(desiredInterval) * time.Millisecond
	default:
		return fmt.Errorf("heartbeat interval (%d) is invalid", desiredInterval)
	}

	return nil
}

// SetOutputBuffer 设置缓冲区超时时间
func (c *clientV2) SetOutputBuffer(desiredSize int, desiredTimeout int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case desiredTimeout == -1:
		c.OutputBufferTimeout = 0
	case desiredTimeout == 0:
		// do nothing (use default)
	case true &&
		desiredTimeout >= int(c.nsqd.getOpts().MinOutputBufferTimeout/time.Millisecond) &&
		desiredTimeout <= int(c.nsqd.getOpts().MaxOutputBufferTimeout/time.Millisecond):

		c.OutputBufferTimeout = time.Duration(desiredTimeout) * time.Millisecond
	default:
		return fmt.Errorf("output buffer timeout (%d) is invalid", desiredTimeout)
	}

	switch {
	case desiredSize == -1:
		// effectively no buffer (every write will go directly to the wrapped net.Conn)
		c.OutputBufferSize = 1
		c.OutputBufferTimeout = 0
	case desiredSize == 0:
		// do nothing (use default)
	case desiredSize >= 64 && desiredSize <= int(c.nsqd.getOpts().MaxOutputBufferSize):
		c.OutputBufferSize = desiredSize
	default:
		return fmt.Errorf("output buffer size (%d) is invalid", desiredSize)
	}

	if desiredSize != 0 {
		err := c.Writer.Flush()
		if err != nil {
			return err
		}
		c.Writer = bufio.NewWriterSize(c.Conn, c.OutputBufferSize)
	}

	return nil
}

// SetSampleRate 设置采样率 随机丢弃报文的比例
func (c *clientV2) SetSampleRate(sampleRate int32) error {
	if sampleRate < 0 || sampleRate > 99 {
		return fmt.Errorf("sample rate (%d) is invalid", sampleRate)
	}
	atomic.StoreInt32(&c.SampleRate, sampleRate)
	return nil
}

func (c *clientV2) SetMsgTimeout(msgTimeout int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case msgTimeout == 0:
		// do nothing (use default)
	case msgTimeout >= 1000 &&
		msgTimeout <= int(c.nsqd.getOpts().MaxMsgTimeout/time.Millisecond):
		c.MsgTimeout = time.Duration(msgTimeout) * time.Millisecond
	default:
		return fmt.Errorf("msg timeout (%d) is invalid", msgTimeout)
	}

	return nil
}

// UpgradeTLS 升级成tls
func (c *clientV2) UpgradeTLS() error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	tlsConn := tls.Server(c.Conn, c.nsqd.tlsConfig)
	tlsConn.SetDeadline(time.Now().Add(5 * time.Second))
	err := tlsConn.Handshake()
	if err != nil {
		return err
	}
	c.tlsConn = tlsConn

	c.Reader = bufio.NewReaderSize(c.tlsConn, defaultBufferSize)
	c.Writer = bufio.NewWriterSize(c.tlsConn, c.OutputBufferSize)

	atomic.StoreInt32(&c.TLS, 1)

	return nil
}

// UpgradeDeflate 读写报文升级成压缩报文
func (c *clientV2) UpgradeDeflate(level int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = bufio.NewReaderSize(flate.NewReader(conn), defaultBufferSize)

	fw, _ := flate.NewWriter(conn, level)
	c.flateWriter = fw
	c.Writer = bufio.NewWriterSize(fw, c.OutputBufferSize)

	atomic.StoreInt32(&c.Deflate, 1)

	return nil
}

// UpgradeSnappy 读写报文升级成压缩报文
func (c *clientV2) UpgradeSnappy() error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = bufio.NewReaderSize(snappy.NewReader(conn), defaultBufferSize)
	c.Writer = bufio.NewWriterSize(snappy.NewWriter(conn), c.OutputBufferSize)

	atomic.StoreInt32(&c.Snappy, 1)

	return nil
}

// Flush 将任何缓冲数据写入底层io.Writer。
func (c *clientV2) Flush() error {
	var zeroTime time.Time
	if c.HeartbeatInterval > 0 {
		c.SetWriteDeadline(time.Now().Add(c.HeartbeatInterval))
	} else {
		c.SetWriteDeadline(zeroTime)
	}

	err := c.Writer.Flush()
	if err != nil {
		return err
	}

	// 压缩的链接
	if c.flateWriter != nil {
		return c.flateWriter.Flush()
	}

	return nil
}

// QueryAuthd 身份认证
func (c *clientV2) QueryAuthd() error {
	// 客户端地址
	remoteIP, _, err := net.SplitHostPort(c.String())
	if err != nil {
		return err
	}

	// 客户端是否使用tls链接
	tlsEnabled := atomic.LoadInt32(&c.TLS) == 1
	commonName := "" // tls证书信息
	if tlsEnabled {
		tlsConnState := c.tlsConn.ConnectionState()
		if len(tlsConnState.PeerCertificates) > 0 {
			commonName = tlsConnState.PeerCertificates[0].Subject.CommonName
		}
	}

	// 身份验证
	authState, err := auth.QueryAnyAuthd(c.nsqd.getOpts().AuthHTTPAddresses,
		remoteIP, tlsEnabled, commonName, c.AuthSecret,
		c.nsqd.getOpts().HTTPClientConnectTimeout,
		c.nsqd.getOpts().HTTPClientRequestTimeout)
	if err != nil {
		return err
	}
	c.AuthState = authState // 设置认证的结果信息
	return nil
}

// Auth 身份认证
func (c *clientV2) Auth(secret string) error {
	c.AuthSecret = secret
	return c.QueryAuthd()
}

// IsAuthorized 是否对topic，channel授权
func (c *clientV2) IsAuthorized(topic, channel string) (bool, error) {
	if c.AuthState == nil {
		return false, nil
	}
	// 授权是否已过期，过期重新认证授权
	if c.AuthState.IsExpired() {
		err := c.QueryAuthd()
		if err != nil {
			return false, err
		}
	}
	// 校验是否已对topic,channel授权
	if c.AuthState.IsAllowed(topic, channel) {
		return true, nil
	}
	return false, nil
}

// HasAuthorizations 是否已进行身份校验
func (c *clientV2) HasAuthorizations() bool {
	if c.AuthState != nil {
		return len(c.AuthState.Authorizations) != 0
	}
	return false
}
