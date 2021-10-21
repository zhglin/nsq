package protocol

type ChildErr interface {
	Parent() error
}

// ClientErr provides a way for NSQ daemons to log a human reabable
// error string and return a machine readable string to the client.
//
// see docs/protocol.md for error codes by command
type ClientErr struct {
	ParentErr error
	Code      string
	Desc      string
}

// Error returns the machine readable form
func (e *ClientErr) Error() string {
	return e.Code + " " + e.Desc
}

// Parent returns the parent error
func (e *ClientErr) Parent() error {
	return e.ParentErr
}

// NewClientErr creates a ClientErr with the supplied human and machine readable strings
func NewClientErr(parent error, code string, description string) *ClientErr {
	return &ClientErr{parent, code, description}
}

// FatalClientErr 客户端错误
type FatalClientErr struct {
	ParentErr error  // 原始错误
	Code      string // 包装下code
	Desc      string // 描述
}

// Error returns the machine readable form
func (e *FatalClientErr) Error() string {
	return e.Code + " " + e.Desc
}

// Parent returns the parent error
func (e *FatalClientErr) Parent() error {
	return e.ParentErr
}

// NewFatalClientErr NewClientErr creates a ClientErr with the supplied human and machine readable strings
// NewFatalClientErr使用提供的人和机器可读字符串创建一个FatalClientErr
// 就是包装下err
func NewFatalClientErr(parent error, code string, description string) *FatalClientErr {
	return &FatalClientErr{parent, code, description}
}
