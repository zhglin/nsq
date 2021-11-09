package nsqd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/dirlock"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/statsd"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

type errStore struct {
	err error
}

type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	// 64位原子变量需要首先在32位平台上正确对齐
	clientIDSequence int64 // client的自增序列号

	sync.RWMutex
	ctx context.Context
	// ctxCancel cancels a context that main() is waiting on
	ctxCancel context.CancelFunc

	opts atomic.Value // 配置选项

	dl        *dirlock.DirLock // 工作目录的锁处理
	isLoading int32            // 启动中 从本地文件加载topic相关元数据
	isExiting int32
	errValue  atomic.Value // 记录操作中的error
	startTime time.Time    // 当前时间

	topicMap map[string]*Topic // topic信息 name=>Topic

	lookupPeers atomic.Value // lookupd的链接

	tcpServer     *tcpServer   // tcp服务端 协议处理
	tcpListener   net.Listener // tcp的Listener
	httpListener  net.Listener // http的Listener
	httpsListener net.Listener // https的Listener
	tlsConfig     *tls.Config

	poolSize int // 处理延迟消息以及inFlight消息的协程数

	notifyChan           chan interface{} // channel的变更通知
	optsNotificationChan chan struct{}    // NSQLookupdTCPAddresses配置的变更通知
	exitChan             chan int         // nsqd退出的通知
	waitGroup            util.WaitGroupWrapper

	ci *clusterinfo.ClusterInfo // lookupd的操作接口
}

// New 创建NSQD
func New(opts *Options) (*NSQD, error) {
	var err error

	// 内部存储数据的目录
	dataPath := opts.DataPath
	if opts.DataPath == "" {
		cwd, _ := os.Getwd()
		dataPath = cwd
	}
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}

	n := &NSQD{
		startTime:            time.Now(),
		topicMap:             make(map[string]*Topic),
		exitChan:             make(chan int),
		notifyChan:           make(chan interface{}),
		optsNotificationChan: make(chan struct{}, 1),
		dl:                   dirlock.New(dataPath), // 目录锁操作
	}
	n.ctx, n.ctxCancel = context.WithCancel(context.Background())
	httpcli := http_api.NewClient(nil, opts.HTTPClientConnectTimeout, opts.HTTPClientRequestTimeout)
	n.ci = clusterinfo.New(n.logf, httpcli)

	n.lookupPeers.Store([]*lookupPeer{})

	n.swapOpts(opts) // 保存配置选项
	n.errValue.Store(errStore{})

	err = n.dl.Lock()
	if err != nil {
		return nil, fmt.Errorf("failed to lock data-path: %v", err)
	}

	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		return nil, errors.New("--max-deflate-level must be [1,9]")
	}

	if opts.ID < 0 || opts.ID >= 1024 {
		return nil, errors.New("--node-id must be [0,1024)")
	}

	if opts.TLSClientAuthPolicy != "" && opts.TLSRequired == TLSNotRequired {
		opts.TLSRequired = TLSRequired
	}

	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config - %s", err)
	}
	if tlsConfig == nil && opts.TLSRequired != TLSNotRequired {
		return nil, errors.New("cannot require TLS client connections without TLS key and cert")
	}
	n.tlsConfig = tlsConfig

	for _, v := range opts.E2EProcessingLatencyPercentiles {
		if v <= 0 || v > 1 {
			return nil, fmt.Errorf("invalid E2E processing latency percentile: %v", v)
		}
	}

	n.logf(LOG_INFO, version.String("nsqd"))
	n.logf(LOG_INFO, "ID: %d", opts.ID)

	// tcp http https服务
	n.tcpServer = &tcpServer{nsqd: n}
	n.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	if opts.HTTPAddress != "" {
		n.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
		if err != nil {
			return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPAddress, err)
		}
	}
	if n.tlsConfig != nil && opts.HTTPSAddress != "" {
		n.httpsListener, err = tls.Listen("tcp", opts.HTTPSAddress, n.tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPSAddress, err)
		}
	}
	if opts.BroadcastHTTPPort == 0 {
		opts.BroadcastHTTPPort = n.RealHTTPAddr().Port
	}

	if opts.BroadcastTCPPort == 0 {
		opts.BroadcastTCPPort = n.RealTCPAddr().Port
	}

	if opts.StatsdPrefix != "" {
		var port string = fmt.Sprint(opts.BroadcastHTTPPort)
		statsdHostKey := statsd.HostKey(net.JoinHostPort(opts.BroadcastAddress, port))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
	}

	return n, nil
}

// 获取配置选项
func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}

// 保存配置选项
func (n *NSQD) swapOpts(opts *Options) {
	n.opts.Store(opts)
}

// NSQLookupdTCPAddresses的变更
func (n *NSQD) triggerOptsNotification() {
	select {
	case n.optsNotificationChan <- struct{}{}:
	default:
	}
}

func (n *NSQD) RealTCPAddr() *net.TCPAddr {
	if n.tcpListener == nil {
		return &net.TCPAddr{}
	}
	return n.tcpListener.Addr().(*net.TCPAddr)

}

func (n *NSQD) RealHTTPAddr() *net.TCPAddr {
	if n.httpListener == nil {
		return &net.TCPAddr{}
	}
	return n.httpListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) RealHTTPSAddr() *net.TCPAddr {
	if n.httpsListener == nil {
		return &net.TCPAddr{}
	}
	return n.httpsListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) SetHealth(err error) {
	n.errValue.Store(errStore{err: err})
}

func (n *NSQD) IsHealthy() bool {
	return n.GetError() == nil
}

func (n *NSQD) GetError() error {
	errValue := n.errValue.Load()
	return errValue.(errStore).err
}

func (n *NSQD) GetHealth() string {
	err := n.GetError()
	if err != nil {
		return fmt.Sprintf("NOK - %s", err)
	}
	return "OK"
}

func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

func (n *NSQD) Main() error {
	exitCh := make(chan error) // 异常信息
	var once sync.Once

	// 退出函数
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				n.logf(LOG_FATAL, "%s", err)
			}
			exitCh <- err
		})
	}

	// tcp服务
	n.waitGroup.Wrap(func() {
		exitFunc(protocol.TCPServer(n.tcpListener, n.tcpServer, n.logf))
	})
	if n.httpListener != nil {
		httpServer := newHTTPServer(n, false, n.getOpts().TLSRequired == TLSRequired)
		n.waitGroup.Wrap(func() {
			exitFunc(http_api.Serve(n.httpListener, httpServer, "HTTP", n.logf))
		})
	}
	if n.httpsListener != nil {
		httpsServer := newHTTPServer(n, true, true)
		n.waitGroup.Wrap(func() {
			exitFunc(http_api.Serve(n.httpsListener, httpsServer, "HTTPS", n.logf))
		})
	}

	// 定时处理延迟消息以及inFlight消息
	n.waitGroup.Wrap(n.queueScanLoop)
	// lookupd链接
	n.waitGroup.Wrap(n.lookupLoop)
	if n.getOpts().StatsdAddress != "" {
		n.waitGroup.Wrap(n.statsdLoop)
	}

	err := <-exitCh
	return err
}

// topic,channel元数据
type meta struct {
	Topics []struct {
		Name     string `json:"name"`
		Paused   bool   `json:"paused"`
		Channels []struct {
			Name   string `json:"name"`
			Paused bool   `json:"paused"`
		} `json:"channels"`
	} `json:"topics"`
}

// topic,channel元数据文件
func newMetadataFile(opts *Options) string {
	return path.Join(opts.DataPath, "nsqd.dat")
}

// 读取文件内容
func readOrEmpty(fn string) ([]byte, error) {
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		// 文件已存在的情况下读取错误
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read metadata from %s - %s", fn, err)
		}
	}
	return data, nil
}

// 同步写入文件
func writeSyncFile(fn string, data []byte) error {
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	return err
}

// LoadMetadata 读取topic,channel元数据
func (n *NSQD) LoadMetadata() error {
	atomic.StoreInt32(&n.isLoading, 1) // 标记为启动中
	defer atomic.StoreInt32(&n.isLoading, 0)

	// 元数据文件名
	fn := newMetadataFile(n.getOpts())

	// 读取文件内容
	data, err := readOrEmpty(fn)
	if err != nil {
		return err
	}
	if data == nil {
		return nil // fresh start
	}

	var m meta
	err = json.Unmarshal(data, &m)
	if err != nil {
		return fmt.Errorf("failed to parse metadata in %s - %s", fn, err)
	}

	for _, t := range m.Topics {
		// 校验topicName
		if !protocol.IsValidTopicName(t.Name) {
			n.logf(LOG_WARN, "skipping creation of invalid topic %s", t.Name)
			continue
		}
		topic := n.GetTopic(t.Name)
		if t.Paused { // 是否暂停
			topic.Pause()
		}
		for _, c := range t.Channels {
			if !protocol.IsValidChannelName(c.Name) {
				n.logf(LOG_WARN, "skipping creation of invalid channel %s", c.Name)
				continue
			}
			channel := topic.GetChannel(c.Name)
			if c.Paused {
				channel.Pause()
			}
		}
		topic.Start() // 启动topic的通知
	}
	return nil
}

// PersistMetadata 记录当前的节点的topic,channel元数据
func (n *NSQD) PersistMetadata() error {
	// persist metadata about what topics/channels we have, across restarts
	// 在重启时，保持关于我们拥有的主题/通道的元数据
	fileName := newMetadataFile(n.getOpts())

	n.logf(LOG_INFO, "NSQ: persisting topic/channel metadata to %s", fileName)

	js := make(map[string]interface{})
	topics := []interface{}{}
	for _, topic := range n.topicMap {
		if topic.ephemeral { // 跳过临时topic
			continue
		}
		topicData := make(map[string]interface{})
		topicData["name"] = topic.name
		topicData["paused"] = topic.IsPaused()
		channels := []interface{}{}
		topic.Lock()
		for _, channel := range topic.channelMap {
			if channel.ephemeral { // 跳过临时channel
				continue
			}
			channel.Lock()
			channelData := make(map[string]interface{})
			channelData["name"] = channel.name
			channelData["paused"] = channel.IsPaused()
			channel.Unlock()
			channels = append(channels, channelData)
		}
		topic.Unlock()
		topicData["channels"] = channels
		topics = append(topics, topicData)
	}
	js["version"] = version.Binary
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	err = writeSyncFile(tmpFileName, data)
	if err != nil {
		return err
	}
	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}
	// technically should fsync DataPath here
	// 技术上应该在这里fsync DataPath
	return nil
}

func (n *NSQD) Exit() {
	if !atomic.CompareAndSwapInt32(&n.isExiting, 0, 1) {
		// avoid double call
		return
	}
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}

	if n.tcpServer != nil {
		n.tcpServer.Close()
	}

	if n.httpListener != nil {
		n.httpListener.Close()
	}

	if n.httpsListener != nil {
		n.httpsListener.Close()
	}

	n.Lock()
	err := n.PersistMetadata()
	if err != nil {
		n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
	}
	n.logf(LOG_INFO, "NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close()
	}
	n.Unlock()

	n.logf(LOG_INFO, "NSQ: stopping subsystems")
	close(n.exitChan)
	n.waitGroup.Wait()
	n.dl.Unlock()
	n.logf(LOG_INFO, "NSQ: bye")
	n.ctxCancel()
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
// GetTopic执行一个线程安全操作，返回一个指向Topic对象(可能是新的)的指针。
func (n *NSQD) GetTopic(topicName string) *Topic {
	// most likely we already have this topic, so try read lock first
	// 读锁，很可能我们已经有了这个主题
	n.RLock()
	t, ok := n.topicMap[topicName]
	n.RUnlock()
	if ok {
		return t
	}

	// 写锁时再尝试读取一次，防止被写入
	n.Lock()
	t, ok = n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	}

	// 创建Topic 并记录
	deleteCallback := func(t *Topic) {
		n.DeleteExistingTopic(t.name)
	}
	t = NewTopic(topicName, n, deleteCallback)
	n.topicMap[topicName] = t

	n.Unlock()

	n.logf(LOG_INFO, "TOPIC(%s): created", t.name)
	// topic is created but messagePump not yet started

	// if this topic was created while loading metadata at startup don't do any further initialization
	// (topic will be "started" after loading completes)
	// 如果这个topic是在启动时加载元数据时创建的，不要做任何进一步的初始化(topic将在加载完成后被“启动”)
	if atomic.LoadInt32(&n.isLoading) == 1 {
		return t
	}

	// if using lookupd, make a blocking call to get channels and immediately create them
	// to ensure that all channels receive published messages
	// 如果使用lookupd，则阻塞调用获取channel，并立即创建channel，以确保所有通道都接收到已发布的消息
	lookupdHTTPAddrs := n.lookupdHTTPAddrs()
	if len(lookupdHTTPAddrs) > 0 {
		channelNames, err := n.ci.GetLookupdTopicChannels(t.name, lookupdHTTPAddrs)
		if err != nil {
			n.logf(LOG_WARN, "failed to query nsqlookupd for channels to pre-create for topic %s - %s", t.name, err)
		}
		for _, channelName := range channelNames {
			if strings.HasSuffix(channelName, "#ephemeral") {
				continue // do not create ephemeral channel with no consumer client
			}
			t.GetChannel(channelName)
		}
	} else if len(n.getOpts().NSQLookupdTCPAddresses) > 0 {
		n.logf(LOG_ERROR, "no available nsqlookupd to query for channels to pre-create for topic %s", t.name)
	}

	// now that all channels are added, start topic messagePump
	t.Start()
	return t
}

// GetExistingTopic gets a topic only if it exists
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
// 删除topic
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	// 在关闭之前删除并清空所有通道和主题本身(这样我们不会留下任何消息)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	// 我们在从下面的映射中删除主题(没有锁)之前这样做，以便任何传入的写入都将出错，而不会创建新的主题来强制排序
	// 先关闭，后删除，防止再创建topic
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

// Notify topic,channel元数据的变更记录
func (n *NSQD) Notify(v interface{}, persist bool) {
	// since the in-memory metadata is incomplete,
	// should not persist metadata while loading it.
	// nsqd will call `PersistMetadata` it after loading
	// 由于内存中的元数据是不完整的，所以在加载元数据时不应该持久化它。nsqd将在加载后调用' PersistMetadata '
	loading := atomic.LoadInt32(&n.isLoading) == 1
	n.waitGroup.Wrap(func() {
		// by selecting on exitChan we guarantee that
		// we do not block exit, see issue #123
		select {
		case <-n.exitChan:
		case n.notifyChan <- v:
			if loading || !persist {
				return
			}
			n.Lock()
			err := n.PersistMetadata()
			if err != nil {
				n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
			}
			n.Unlock()
		}
	})
}

// channels returns a flat slice of all channels in all topics
// 返回所有topic中所有channel的切片
func (n *NSQD) channels() []*Channel {
	var channels []*Channel
	n.RLock()
	for _, t := range n.topicMap {
		t.RLock()
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		t.RUnlock()
	}
	n.RUnlock()
	return channels
}

// resizePool adjusts the size of the pool of queueScanWorker goroutines
//
// 	1 <= pool <= min(num * 0.25, QueueScanWorkerPoolMax)
// 调整queueScanWorker goroutine池的大小 num:总channel数
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	// 需要开启的协程数
	idealPoolSize := int(float64(num) * 0.25)
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > n.getOpts().QueueScanWorkerPoolMax {
		idealPoolSize = n.getOpts().QueueScanWorkerPoolMax
	}
	for {
		if idealPoolSize == n.poolSize { // 刚好相等
			break
		} else if idealPoolSize < n.poolSize { // 已开的协程数超过限制
			// contract
			closeCh <- 1 // 通知queueScanWorker关闭一个
			n.poolSize--
		} else {
			// expand 增加一个协程处理
			n.waitGroup.Wrap(func() {
				n.queueScanWorker(workCh, responseCh, closeCh)
			})
			n.poolSize++
		}
	}
}

// queueScanWorker receives work (in the form of a channel) from queueScanLoop
// and processes the deferred and in-flight queues
// 从queueScanLoop接收工作(以通道的形式)，并处理延迟的和正在运行的队列
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		case c := <-workCh:
			now := time.Now().UnixNano()
			dirty := false
			// 未FIN的消息处理
			if c.processInFlightQueue(now) {
				dirty = true
			}
			if c.processDeferredQueue(now) {
				dirty = true
			}
			responseCh <- dirty
		case <-closeCh: // 协程退出
			return
		}
	}
}

// queueScanLoop runs in a single goroutine to process in-flight and deferred
// priority queues. It manages a pool of queueScanWorker (configurable max of
// QueueScanWorkerPoolMax (default: 4)) that process channels concurrently.
//
// It copies Redis's probabilistic expiration algorithm: it wakes up every
// QueueScanInterval (default: 100ms) to select a random QueueScanSelectionCount
// (default: 20) channels from a locally cached list (refreshed every
// QueueScanRefreshInterval (default: 5s)).
//
// If either of the queues had work to do the channel is considered "dirty".
//
// If QueueScanDirtyPercent (default: 25%) of the selected channels were dirty,
// the loop continues without sleep.
// queueScanLoop在单个goroutine中运行，以处理动态队列和延迟优先级队列。
// 它管理一个并发处理通道的queueScanWorker池(可配置的QueueScanWorkerPoolMax的最大值(默认值:4))。
// 它复制Redis的概率过期算法:它唤醒每个QueueScanInterval(默认:100ms)从本地缓存列表中选择一个随机的QueueScanSelectionCount(默认:20)通道(刷新每个QueueScanRefreshInterval(默认:5s))。
// 如果其中一个队列有工作要做，则认为通道是“脏的”。
// 如果所选通道的QueueScanDirtyPercent(默认值:25%)是dirty，则循环将继续而不休眠。
func (n *NSQD) queueScanLoop() {
	workCh := make(chan *Channel, n.getOpts().QueueScanSelectionCount)
	responseCh := make(chan bool, n.getOpts().QueueScanSelectionCount)
	closeCh := make(chan int)

	workTicker := time.NewTicker(n.getOpts().QueueScanInterval)
	refreshTicker := time.NewTicker(n.getOpts().QueueScanRefreshInterval)

	// 获取所有channel
	channels := n.channels()
	// 初始化工作协程
	n.resizePool(len(channels), workCh, responseCh, closeCh)

	for {
		select {
		case <-workTicker.C: // 定时进行扫描
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C: // 定时重置工作协程数量
			channels = n.channels()
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-n.exitChan: // 退出
			goto exit
		}

		// 需要扫描的channel数量
		num := n.getOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		// 随机选出num个channel写入workCh
		for _, i := range util.UniqRands(num, len(channels)) {
			workCh <- channels[i]
		}

		numDirty := 0
		for i := 0; i < num; i++ {
			if <-responseCh { // for循环控制次数 不会一直阻塞
				numDirty++
			}
		}

		// 校验成功的百分比，dirty比例太高重试
		if float64(numDirty)/float64(num) > n.getOpts().QueueScanDirtyPercent {
			goto loop
		}
	}

exit:
	n.logf(LOG_INFO, "QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}

func buildTLSConfig(opts *Options) (*tls.Config, error) {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil, nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, err
	}
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
		MinVersion:   opts.TLSMinVersion,
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := ioutil.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

// IsAuthEnabled 是否需要身份校验
func (n *NSQD) IsAuthEnabled() bool {
	return len(n.getOpts().AuthHTTPAddresses) != 0
}

// Context returns a context that will be canceled when nsqd initiates the shutdown
func (n *NSQD) Context() context.Context {
	return n.ctx
}
