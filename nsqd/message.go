package nsqd

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

type MessageID [MsgIDLength]byte

// Message 消息体
type Message struct {
	ID        MessageID // 消息id
	Body      []byte    // 消息内容
	Timestamp int64     // 创建的当前时间戳
	Attempts  uint16    // 发送到客户端的次数

	// for in-flight handling
	deliveryTS time.Time     // 发送的当前时间
	clientID   int64         // 发送给的客户端标识
	pri        int64         // 超时时间绝对时间
	index      int           // 在inFlightPqueue中的下标
	deferred   time.Duration // 延迟消息的延迟时间
}

// NewMessage 新建消息体
func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

// WriteTo 消息编码
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	n, err := w.Write(buf[:]) // 写入timestamp,attempts
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:]) // 写入id
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body) // 写入body
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// decodeMessage deserializes data (as []byte) and creates a new Message
// message format:
// [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
// |       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
// |       8-byte         ||    ||                 16-byte                      || N-byte
// ------------------------------------------------------------------------------------------...
//   nanosecond timestamp    ^^                   message ID                       message body
//                        (uint16)
//                         2-byte
//                        attempts
// 解析从磁盘文件读取的消息
func decodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	copy(msg.ID[:], b[10:10+MsgIDLength])
	msg.Body = b[10+MsgIDLength:]

	return &msg, nil
}

// 写入消息到后端channel
func writeMessageToBackend(msg *Message, bq BackendQueue) error {
	buf := bufferPoolGet()
	defer bufferPoolPut(buf)
	// 消息写入buf
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}
	// buf写入后端队列
	return bq.Put(buf.Bytes())
}
