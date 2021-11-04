//go:build !windows && !illumos
// +build !windows,!illumos

package dirlock

import (
	"fmt"
	"os"
	"syscall"
)

type DirLock struct {
	dir string   // 目录地址
	f   *os.File // dir对应的文件描述符
}

func New(dir string) *DirLock {
	return &DirLock{
		dir: dir,
	}
}

// Lock 锁定当前目录，只允许当前进程进行操作
// LOCK_EX 建立互斥锁定。一个文件同时只有一个互斥锁定。
// LOCK_NB 无法建立锁定时，此操作可不被阻断，马上返回进程。
func (l *DirLock) Lock() error {
	f, err := os.Open(l.dir)
	if err != nil {
		return err
	}
	l.f = f
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return fmt.Errorf("cannot flock directory %s - %s (possibly in use by another instance of nsqd)", l.dir, err)
	}
	return nil
}

// Unlock 解锁当前目录
func (l *DirLock) Unlock() error {
	defer l.f.Close()
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}
