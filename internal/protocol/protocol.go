package protocol

import (
	"encoding/binary"
	"io"
	"net"
)

type Client interface {
	Close() error
}

// Protocol describes the basic behavior of any protocol in the system
// 描述系统中任何协议的基本行为
type Protocol interface {
	NewClient(net.Conn) Client
	IOLoop(Client) error
}

// SendResponse is a server side utility function to prefix data with a length header
// and write to the supplied Writer
// SendResponse是一个服务器端实用程序函数，用于为数据添加长度标头前缀，并将数据写入所提供的Writer
func SendResponse(w io.Writer, data []byte) (int, error) {
	// 先写入长度
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	// 再写入内容
	n, err := w.Write(data)
	if err != nil {
		return 0, err
	}

	return (n + 4), nil
}

// SendFramedResponse is a server side utility function to prefix data with a length header
// and frame header and write to the supplied Writer
// 是一个服务器端实用函数，用于在数据前添加长度报头和帧报头，并写入所提供的Writer
func SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4

	// 报文长度
	binary.BigEndian.PutUint32(beBuf, size)
	n, err := w.Write(beBuf)
	if err != nil {
		return n, err
	}

	// 报文类型
	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	n, err = w.Write(beBuf)
	if err != nil {
		return n + 4, err
	}

	// 报文内容
	n, err = w.Write(data)
	return n + 8, err
}
