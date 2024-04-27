package selfsync

import (
	"runtime"
	"sync"
	"sync/atomic"
)

/*
参考自ants自旋锁代码
*/
type spinLock uint32

const maxBackoff = 16

// 自旋锁代码
func (sl *spinLock) Lock() {
	backoff := 1
	for !atomic.CompareAndSwapUint32((*uint32)(sl), 0, 1) {
		// Leverage the exponential backoff algorithm, see https://en.wikipedia.org/wiki/Exponential_backoff.
		//通过指数退让机制，来让锁的访问次数减少，但不能少于16
		for i := 0; i < backoff; i++ {
			runtime.Gosched()
		}
		if backoff < maxBackoff {
			backoff <<= 1
		}
	}
}

func (sl *spinLock) Unlock() {
	atomic.StoreUint32((*uint32)(sl), 0)
}

// NewSpinLock instantiates a spin-lock.
func NewSpinLock() sync.Locker {
	return new(spinLock)
}
