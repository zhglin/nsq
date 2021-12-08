package nsqd

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"

	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/pqueue"
	"github.com/nsqio/nsq/internal/quantile"
)

type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats(string) ClientStats
	Empty()
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
//
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeuing, etc.
type Channel struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount uint64 // 重新写入此channel的消息数量
	messageCount uint64 // 写入此channel的消息数量
	timeoutCount uint64 // 超时消息数量

	sync.RWMutex

	topicName string // topic名称
	name      string // channel名称
	nsqd      *NSQD

	backend BackendQueue // 后端队列

	memoryMsgChan chan *Message // 内存队列
	exitFlag      int32
	exitMutex     sync.RWMutex

	// state tracking
	clients        map[int64]Consumer // 已订阅的客户端
	paused         int32              // 是否暂停，不再向client发送消息
	ephemeral      bool               // 是否临时队列
	deleteCallback func(*Channel)     // 删除channel的函数，临时channel自动删除
	deleter        sync.Once

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile

	// TODO: these can be DRYd up
	deferredMessages map[MessageID]*pqueue.Item // 延时队列 消息id=>消息
	deferredPQ       pqueue.PriorityQueue       // 延时队列 优先级队列
	deferredMutex    sync.Mutex                 // 延时消息锁
	inFlightMessages map[MessageID]*Message     // 飞行中的消息 消息id=>消息 一个channel的消息分给多个client消费
	inFlightPQ       inFlightPqueue             // 飞行中的消息 优先级队列
	inFlightMutex    sync.Mutex                 // 飞行中的消息 添加优先级队列的互斥锁
}

// NewChannel creates a new instance of the Channel type and returns a pointer
// 创建一个Channel类型的新实例并返回一个指针
func NewChannel(topicName string, channelName string, nsqd *NSQD,
	deleteCallback func(*Channel)) *Channel {

	c := &Channel{
		topicName:      topicName,
		name:           channelName,
		memoryMsgChan:  nil,
		clients:        make(map[int64]Consumer),
		deleteCallback: deleteCallback,
		nsqd:           nsqd,
	}
	// create mem-queue only if size > 0 (do not use unbuffered chan)
	// 仅当大小为> 0时创建mem-queue(不要使用未缓冲的chan)
	if nsqd.getOpts().MemQueueSize > 0 {
		c.memoryMsgChan = make(chan *Message, nsqd.getOpts().MemQueueSize)
	}
	if len(nsqd.getOpts().E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			nsqd.getOpts().E2EProcessingLatencyWindowTime,
			nsqd.getOpts().E2EProcessingLatencyPercentiles,
		)
	}

	c.initPQ()

	// 临时队列对应不同的后端队列实现
	if strings.HasSuffix(channelName, "#ephemeral") {
		c.ephemeral = true
		c.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		// backend names, for uniqueness, automatically include the topic...
		backendName := getBackendName(topicName, channelName)
		c.backend = diskqueue.New(
			backendName,
			nsqd.getOpts().DataPath,
			nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			nsqd.getOpts().SyncEvery,
			nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}

	// 通知nsqd持久化
	c.nsqd.Notify(c, !c.ephemeral)

	return c
}

// 重置延迟，inFlight队列
func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.nsqd.getOpts().MemQueueSize)/10))

	// 飞行中的消息信息
	c.inFlightMutex.Lock()
	c.inFlightMessages = make(map[MessageID]*Message)
	c.inFlightPQ = newInFlightPqueue(pqSize) // 优先级队列
	c.inFlightMutex.Unlock()

	// 延迟队列
	c.deferredMutex.Lock()
	c.deferredMessages = make(map[MessageID]*pqueue.Item)
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}

// Exiting returns a boolean indicating if this channel is closed/exiting
// 返回一个布尔值，指示此通道是否关闭/退出
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

// Delete empties the channel and closes
// 清空通道并关闭
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close cleanly closes the Channel
// 干净地关闭Channel
func (c *Channel) Close() error {
	return c.exit(false)
}

// 退出
func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()

	// 先标记exiting 后续关闭客户端
	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		c.nsqd.logf(LOG_INFO, "CHANNEL(%s): deleting", c.name)

		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd
		// 因为我们要显式地删除通道(不仅仅是在系统退出时)，所以要将其从被查询对象中注销
		c.nsqd.Notify(c, !c.ephemeral)
	} else {
		c.nsqd.logf(LOG_INFO, "CHANNEL(%s): closing", c.name)
	}

	// this forceably closes client connections
	// 这将强制关闭客户端连接
	c.RLock()
	for _, client := range c.clients {
		client.Close()
	}
	c.RUnlock()

	if deleted {
		// empty the queue (deletes the backend files, too)
		// 清空队列(也删除后端文件)
		c.Empty()
		return c.backend.Delete()
	}

	// write anything leftover to disk
	// 将剩余的内容写入磁盘
	c.flush()
	return c.backend.Close()
}

// Empty 清空channel数据
func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()

	// 重置优先级队列
	c.initPQ()
	for _, client := range c.clients {
		client.Empty() // 回调
	}

	// 清空内存队列
	for {
		select {
		case <-c.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return c.backend.Empty()
}

// flush persists all the messages in internal memory buffers to the backend
// it does not drain inflight/deferred because it is only called in Close()
// 将内部内存缓冲区中的所有消息都持久化到后端，因为它只在Close()中调用，所以不会在inflight/deferred耗尽。
func (c *Channel) flush() error {
	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
		c.nsqd.logf(LOG_INFO, "CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages))
	}

	// 持久化内存队列
	for {
		select {
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(msg, c.backend)
			if err != nil {
				c.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

	// 写入inFlight消息，延迟消息
finish:
	c.inFlightMutex.Lock()
	for _, msg := range c.inFlightMessages {
		err := writeMessageToBackend(msg, c.backend)
		if err != nil {
			c.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		err := writeMessageToBackend(msg, c.backend)
		if err != nil {
			c.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.deferredMutex.Unlock()

	return nil
}

func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan)) + c.backend.Depth()
}

// Pause 暂停channel
func (c *Channel) Pause() error {
	return c.doPause(true)
}

// UnPause 取消暂停channel
func (c *Channel) UnPause() error {
	return c.doPause(false)
}

// 修改是否暂停的状态
func (c *Channel) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	// 回调各个client
	c.RLock()
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
	return nil
}

// IsPaused channel是否已暂停
func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// PutMessage writes a Message to the queue
// 将Message写入队列
func (c *Channel) PutMessage(m *Message) error {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()
	if c.Exiting() {
		return errors.New("exiting")
	}
	// 写入
	err := c.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}

// 写入消息到channel
func (c *Channel) put(m *Message) error {
	select {
	case c.memoryMsgChan <- m: // 内存队列写不进去就写入后端队列
	default:
		err := writeMessageToBackend(m, c.backend)
		c.nsqd.SetHealth(err) // 在nsqd中记录err
		if err != nil {
			c.nsqd.logf(LOG_ERROR, "CHANNEL(%s): failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}

// PutMessageDeferred 对此channel写入延迟消息
func (c *Channel) PutMessageDeferred(msg *Message, timeout time.Duration) {
	atomic.AddUint64(&c.messageCount, 1)
	c.StartDeferredTimeout(msg, timeout)
}

// TouchMessage resets the timeout for an in-flight message
// 修改单个消息的超时时间
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	// 从优先级队列里取出消息
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)

	// 计算新的超时时间
	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.nsqd.getOpts().MaxMsgTimeout {
		// we would have gone over, set to the max 超过最大超时时间
		newTimeout = msg.deliveryTS.Add(c.nsqd.getOpts().MaxMsgTimeout)
	}

	// 从新写入
	msg.pri = newTimeout.UnixNano()
	err = c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// FinishMessage successfully discards an in-flight message
// 成功丢弃一个飞行中的消息 客户端已FIN
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	// 根据消息id获取消息
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}

	// 从优先级队列删除消息
	c.removeFromInFlightPQ(msg)
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}
	return nil
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
// 重新放入队列,必须在inflight队列中的消息
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	// remove from inflight first 从inflight队列中删除
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	atomic.AddUint64(&c.requeueCount, 1)

	// 直接写入队列
	if timeout == 0 {
		c.exitMutex.RLock()
		if c.Exiting() {
			c.exitMutex.RUnlock()
			return errors.New("exiting")
		}
		err := c.put(msg)
		c.exitMutex.RUnlock()
		return err
	}

	// deferred requeue 延迟队列
	return c.StartDeferredTimeout(msg, timeout)
}

// AddClient adds a client to the Channel's client list
// 将客户端添加到channel的客户端列表
func (c *Channel) AddClient(clientID int64, client Consumer) error {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	// channel已退出
	if c.Exiting() {
		return errors.New("exiting")
	}

	// 已添加
	c.RLock()
	_, ok := c.clients[clientID]
	numClients := len(c.clients)
	c.RUnlock()
	if ok {
		return nil
	}

	// 校验订阅的客户端数量
	maxChannelConsumers := c.nsqd.getOpts().MaxChannelConsumers
	if maxChannelConsumers != 0 && numClients >= maxChannelConsumers {
		return fmt.Errorf("consumers for %s:%s exceeds limit of %d",
			c.topicName, c.name, maxChannelConsumers)
	}

	c.Lock()
	c.clients[clientID] = client
	c.Unlock()
	return nil
}

// RemoveClient removes a client from the Channel's client list
// 从channel的客户端列表中删除客户端
func (c *Channel) RemoveClient(clientID int64) {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	// 已标记exiting 后续会自动关闭client
	if c.Exiting() {
		return
	}

	c.RLock()
	_, ok := c.clients[clientID]
	c.RUnlock()
	if !ok {
		return
	}

	// 只是从clients中删除，并不关闭链接
	c.Lock()
	delete(c.clients, clientID)
	c.Unlock()

	// 如果是临时channel并且没有client直接关闭channel
	if len(c.clients) == 0 && c.ephemeral == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}

// StartInFlightTimeout 发送给客户端
func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	err := c.pushInFlightMessage(msg) // 添加到记录中
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// StartDeferredTimeout 添加延时消息
func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	absTs := time.Now().Add(timeout).UnixNano()       // 绝对时间
	item := &pqueue.Item{Value: msg, Priority: absTs} // 队列元素
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	c.addToDeferredPQ(item)
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
// 以原子方式将消息添加到飞行中的字典中
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightMutex.Unlock()
	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
// 以原子方式从正在运行的字典中删除一条消息并返回
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return nil, errors.New("client does not own message")
	}
	delete(c.inFlightMessages, id)
	c.inFlightMutex.Unlock()
	return msg, nil
}

// 添加到inFlightPQ优先级队列
func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

// 从优先级队列中删除
func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		// 这个项目已经从pqueue中弹出
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}

// 写入到延时消息deferredMessages
func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly 这些map查找是昂贵的
	id := item.Value.(*Message).ID
	_, ok := c.deferredMessages[id]
	if ok {
		c.deferredMutex.Unlock()
		return errors.New("ID already deferred")
	}
	c.deferredMessages[id] = item
	c.deferredMutex.Unlock()
	return nil
}

// 根据消息id获取延迟消息
func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		c.deferredMutex.Unlock()
		return nil, errors.New("ID not deferred")
	}
	delete(c.deferredMessages, id)
	c.deferredMutex.Unlock()
	return item, nil
}

// 添加到延迟队列(优先级队列)
func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)
	c.deferredMutex.Unlock()
}

// 内存中的延迟消息写入channel
func (c *Channel) processDeferredQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		// 获取小于当前时间t的消息，写入channel
		c.deferredMutex.Lock()
		item, _ := c.deferredPQ.PeekAndShift(t)
		c.deferredMutex.Unlock()

		if item == nil {
			goto exit
		}
		dirty = true

		msg := item.Value.(*Message)
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			goto exit
		}
		c.put(msg)
	}

exit:
	return dirty
}

// InFlight中超时未FIN的消息处理 返回是否具有超时的消息
func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		// 小于当前时间的消息堆顶元素 超时的消息
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		dirty = true

		// 删掉记录
		_, err := c.popInFlightMessage(msg.clientID, msg.ID)
		if err != nil {
			goto exit
		}
		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		// 发送超时消息
		if ok {
			client.TimedOutMessage()
		}
		// 重新放入队列
		c.put(msg)
	}

exit:
	return dirty
}
