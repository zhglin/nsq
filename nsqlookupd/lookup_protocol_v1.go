package nsqlookupd

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

// LookupProtocolV1 协议版本号对应的协议解析
type LookupProtocolV1 struct {
	nsqlookupd *NSQLookupd
}

// NewClient 创建客户端链接的信息 返回的是引用
func (p *LookupProtocolV1) NewClient(conn net.Conn) protocol.Client {
	return NewClientV1(conn)
}

// IOLoop 解析并处理报文 报文内容(命令 参数...)
func (p *LookupProtocolV1) IOLoop(c protocol.Client) error {
	var err error
	var line string // 请求报文

	client := c.(*ClientV1)

	reader := bufio.NewReader(client) // 具有缓冲的io
	for {
		// 按'\n'切割报文，tcp粘包
		line, err = reader.ReadString('\n')
		if err != nil { // 已关闭
			break
		}

		line = strings.TrimSpace(line)
		params := strings.Split(line, " ") // 命令 参数

		var response []byte
		response, err = p.Exec(client, reader, params)
		if err != nil {
			ctx := ""
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, err, ctx)

			// 写入错误信息
			_, sendErr := protocol.SendResponse(client, []byte(err.Error()))
			if sendErr != nil {
				p.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, sendErr, ctx)
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			// FatalClientErr类型的错误应该强制关闭连接
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}

		// 写入响应
		if response != nil {
			_, err = protocol.SendResponse(client, response)
			if err != nil {
				break
			}
		}
	}

	// 链接异常 || 报文异常
	p.nsqlookupd.logf(LOG_INFO, "PROTOCOL(V1): [%s] exiting ioloop", client)

	// 删除此链接所有的producer client topic channel
	if client.peerInfo != nil {
		registrations := p.nsqlookupd.DB.LookupRegistrations(client.peerInfo.id)
		for _, r := range registrations {
			if removed, _ := p.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
					client, r.Category, r.Key, r.SubKey)
			}
		}
	}

	return err
}

// Exec 支持的协议命令 REGISTER，UNREGISTER都是针对当前client的
func (p *LookupProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	case "PING": // 值更新client的updateTime
		return p.PING(client, params)
	case "IDENTIFY": // 优先  client的信息
		return p.IDENTIFY(client, reader, params[1:])
	case "REGISTER": // 注册topic channel
		return p.REGISTER(client, reader, params[1:])
	case "UNREGISTER": // 注销topic channel
		return p.UNREGISTER(client, reader, params[1:])
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

// 从参数中解析出来topic channel
func getTopicChan(command string, params []string) (string, string, error) {
	// 必须包含topicName
	if len(params) == 0 {
		return "", "", protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("%s insufficient number of params", command))
	}

	topicName := params[0]
	var channelName string
	if len(params) >= 2 {
		channelName = params[1]
	}

	// topicName校验
	if !protocol.IsValidTopicName(topicName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_TOPIC", fmt.Sprintf("%s topic name '%s' is not valid", command, topicName))
	}

	// channelName校验
	if channelName != "" && !protocol.IsValidChannelName(channelName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL", fmt.Sprintf("%s channel name '%s' is not valid", command, channelName))
	}

	return topicName, channelName, nil
}

// REGISTER 注册topic channel
func (p *LookupProtocolV1) REGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}

	// topic,channel的名称
	topic, channel, err := getTopicChan("REGISTER", params)
	if err != nil {
		return nil, err
	}

	// 注册producer
	if channel != "" {
		key := Registration{"channel", topic, channel}
		if p.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
			p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
	}
	key := Registration{"topic", topic, ""}
	if p.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
		p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
			client, "topic", topic, "")
	}

	return []byte("OK"), nil
}

// UNREGISTER 注销topic channel，只是这个client的topic，channel
func (p *LookupProtocolV1) UNREGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}

	// 获取topic,channel名称
	topic, channel, err := getTopicChan("UNREGISTER", params)
	if err != nil {
		return nil, err
	}

	if channel != "" {
		key := Registration{"channel", topic, channel}
		removed, left := p.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
		// for ephemeral channels, remove the channel as well if it has no producers
		// 对于临时通道，如果没有了producer，也要删除该类型的producer
		if left == 0 && strings.HasSuffix(channel, "#ephemeral") {
			p.nsqlookupd.DB.RemoveRegistration(key)
		}
	} else {
		// no channel was specified so this is a topic unregistration
		// remove all of the channel registrations...
		// normally this shouldn't happen which is why we print a warning message
		// if anything is actually removed
		// 没有指定通道，所以这是一个主题取消注册，删除所有的通道注册…
		// 通常这不会发生，这就是为什么我们打印一个警告消息，如果任何东西实际上被删除
		registrations := p.nsqlookupd.DB.FindRegistrations("channel", topic, "*")
		for _, r := range registrations {
			removed, _ := p.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id)
			if removed {
				p.nsqlookupd.logf(LOG_WARN, "client(%s) unexpected UNREGISTER category:%s key:%s subkey:%s",
					client, "channel", topic, r.SubKey)
			}
		}

		key := Registration{"topic", topic, ""}
		removed, left := p.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "topic", topic, "")
		}
		if left == 0 && strings.HasSuffix(topic, "#ephemeral") {
			p.nsqlookupd.DB.RemoveRegistration(key)
		}
	}

	return []byte("OK"), nil
}

// IDENTIFY 读取并设置client的信息
func (p *LookupProtocolV1) IDENTIFY(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

	if client.peerInfo != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID", "cannot IDENTIFY again")
	}

	// 接着读取 IDENTIFY 的报文
	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen) // 四个字节的长度
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	// 报文内容
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	// Body是一个带有生产者信息的json结构
	peerInfo := PeerInfo{id: client.RemoteAddr().String()}
	err = json.Unmarshal(body, &peerInfo)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	peerInfo.RemoteAddress = client.RemoteAddr().String()

	// require all fields
	// 校验字段
	if peerInfo.BroadcastAddress == "" || peerInfo.TCPPort == 0 || peerInfo.HTTPPort == 0 || peerInfo.Version == "" {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY", "IDENTIFY missing fields")
	}

	// 设置时间戳
	atomic.StoreInt64(&peerInfo.lastUpdate, time.Now().UnixNano())

	p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): IDENTIFY Address:%s TCP:%d HTTP:%d Version:%s",
		client, peerInfo.BroadcastAddress, peerInfo.TCPPort, peerInfo.HTTPPort, peerInfo.Version)

	// 添加生产者
	client.peerInfo = &peerInfo
	if p.nsqlookupd.DB.AddProducer(Registration{"client", "", ""}, &Producer{peerInfo: client.peerInfo}) {
		p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "client", "", "")
	}

	// build a response
	data := make(map[string]interface{})
	data["tcp_port"] = p.nsqlookupd.RealTCPAddr().Port   // tcp端口号
	data["http_port"] = p.nsqlookupd.RealHTTPAddr().Port // http端口号
	data["version"] = version.Binary                     // 协议版本号
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err)
	}
	data["broadcast_address"] = p.nsqlookupd.opts.BroadcastAddress // 默认主机名
	data["hostname"] = hostname                                    // 主机名

	response, err := json.Marshal(data)
	if err != nil {
		p.nsqlookupd.logf(LOG_ERROR, "marshaling %v", data)
		return []byte("OK"), nil
	}
	return response, nil
}

// PING ping命令的处理 更新client的最后时间
func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	// 更新链接的updateTime
	if client.peerInfo != nil {
		// we could get a PING before other commands on the same client connection
		cur := time.Unix(0, atomic.LoadInt64(&client.peerInfo.lastUpdate))
		now := time.Now()
		p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): pinged (last ping %s)", client.peerInfo.id,
			now.Sub(cur))
		atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano())
	}
	return []byte("OK"), nil
}
