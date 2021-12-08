package nsqd

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/version"
)

// 与lookupd建立链接后执行的函数 发送identify命令以及注册所有的topic,channel
func connectCallback(n *NSQD, hostname string) func(*lookupPeer) {
	return func(lp *lookupPeer) {
		ci := make(map[string]interface{})
		ci["version"] = version.Binary
		ci["tcp_port"] = n.getOpts().BroadcastTCPPort
		ci["http_port"] = n.getOpts().BroadcastHTTPPort
		ci["hostname"] = hostname
		ci["broadcast_address"] = n.getOpts().BroadcastAddress

		// 生成identify命令
		cmd, err := nsq.Identify(ci)
		if err != nil {
			lp.Close()
			return
		}
		// 发送identify命令
		resp, err := lp.Command(cmd)
		if err != nil {
			n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
			return
		} else if bytes.Equal(resp, []byte("E_INVALID")) {
			n.logf(LOG_INFO, "LOOKUPD(%s): lookupd returned %s", lp, resp)
			lp.Close()
			return
		} else {
			err = json.Unmarshal(resp, &lp.Info)
			if err != nil {
				n.logf(LOG_ERROR, "LOOKUPD(%s): parsing response - %s", lp, resp)
				lp.Close()
				return
			} else {
				n.logf(LOG_INFO, "LOOKUPD(%s): peer info %+v", lp, lp.Info)
				if lp.Info.BroadcastAddress == "" {
					n.logf(LOG_ERROR, "LOOKUPD(%s): no broadcast address", lp)
				}
			}
		}

		// build all the commands first so we exit the lock(s) as fast as possible
		// 首先构建所有命令，以便我们尽可能快地退出锁
		// 构建所有的topic，channel注册命令
		var commands []*nsq.Command
		n.RLock()
		for _, topic := range n.topicMap {
			topic.RLock()
			if len(topic.channelMap) == 0 {
				commands = append(commands, nsq.Register(topic.name, ""))
			} else {
				for _, channel := range topic.channelMap {
					commands = append(commands, nsq.Register(channel.topicName, channel.name))
				}
			}
			topic.RUnlock()
		}
		n.RUnlock()

		// 发送所有的注册命令
		for _, cmd := range commands {
			n.logf(LOG_INFO, "LOOKUPD(%s): %s", lp, cmd)
			_, err := lp.Command(cmd)
			if err != nil {
				n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
				return
			}
		}
	}
}

// lookup链接
func (n *NSQD) lookupLoop() {
	var lookupPeers []*lookupPeer
	var lookupAddrs []string // 已建立链接的lookupd地址
	connect := true          // 是否已链接各个lookupd

	hostname, err := os.Hostname()
	if err != nil {
		n.logf(LOG_FATAL, "failed to get hostname - %s", err)
		os.Exit(1)
	}

	// for announcements, lookupd determines the host automatically
	// 定时发送心跳
	ticker := time.Tick(15 * time.Second)
	for {
		if connect {
			for _, host := range n.getOpts().NSQLookupdTCPAddresses {
				// 节点已存在
				if in(host, lookupAddrs) {
					continue
				}
				n.logf(LOG_INFO, "LOOKUP(%s): adding peer", host)

				// 创建并链接lookupd
				lookupPeer := newLookupPeer(host, n.getOpts().MaxBodySize, n.logf,
					connectCallback(n, hostname))
				lookupPeer.Command(nil) // start the connection
				lookupPeers = append(lookupPeers, lookupPeer)
				lookupAddrs = append(lookupAddrs, host)
			}
			n.lookupPeers.Store(lookupPeers)
			// select后会重新进行connect(跳过已建立的节点)
			connect = false
		}

		select {
		case <-ticker: // 心跳 异常不删除链接，重新connectCallback
			// send a heartbeat and read a response (read detects closed conns)
			// 发送一个心跳并读取一个响应(读取检测关闭的conns)
			for _, lookupPeer := range lookupPeers {
				n.logf(LOG_DEBUG, "LOOKUPD(%s): sending heartbeat", lookupPeer)
				cmd := nsq.Ping()
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		case val := <-n.notifyChan: // channel变更
			var cmd *nsq.Command
			var branch string

			switch val.(type) {
			case *Channel:
				// notify all nsqlookupds that a new channel exists, or that it's removed
				// 通知所有nsqlookupds有一个新的通道存在，或者它被删除了
				branch = "channel"
				channel := val.(*Channel)
				if channel.Exiting() == true {
					cmd = nsq.UnRegister(channel.topicName, channel.name)
				} else {
					cmd = nsq.Register(channel.topicName, channel.name)
				}
			case *Topic:
				// notify all nsqlookupds that a new topic exists, or that it's removed
				// 通知所有nsqlookupds有新主题存在，或已删除
				branch = "topic"
				topic := val.(*Topic)
				if topic.Exiting() == true {
					cmd = nsq.UnRegister(topic.name, "")
				} else {
					cmd = nsq.Register(topic.name, "")
				}
			}

			// 向所有lookupd发送命令
			for _, lookupPeer := range lookupPeers {
				n.logf(LOG_INFO, "LOOKUPD(%s): %s %s", lookupPeer, branch, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		case <-n.optsNotificationChan: // NSQLookupdTCPAddresses配置的变更
			var tmpPeers []*lookupPeer
			var tmpAddrs []string
			// 从已建立的链接中删掉已不存在的address，这里不会进行添加
			for _, lp := range lookupPeers {
				if in(lp.addr, n.getOpts().NSQLookupdTCPAddresses) {
					tmpPeers = append(tmpPeers, lp)
					tmpAddrs = append(tmpAddrs, lp.addr)
					continue
				}
				n.logf(LOG_INFO, "LOOKUP(%s): removing peer", lp)
				lp.Close()
			}
			lookupPeers = tmpPeers
			lookupAddrs = tmpAddrs
			connect = true // 需要再进行connect
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.logf(LOG_INFO, "LOOKUP: closing")
}

// 字符串是否在字符数组中
func in(s string, lst []string) bool {
	for _, v := range lst {
		if s == v {
			return true
		}
	}
	return false
}

// 获取已建立链接的lookupd地址
func (n *NSQD) lookupdHTTPAddrs() []string {
	var lookupHTTPAddrs []string
	lookupPeers := n.lookupPeers.Load()
	if lookupPeers == nil {
		return nil
	}
	for _, lp := range lookupPeers.([]*lookupPeer) {
		if len(lp.Info.BroadcastAddress) <= 0 {
			continue
		}
		addr := net.JoinHostPort(lp.Info.BroadcastAddress, strconv.Itoa(lp.Info.HTTPPort))
		lookupHTTPAddrs = append(lookupHTTPAddrs, addr)
	}
	return lookupHTTPAddrs
}
