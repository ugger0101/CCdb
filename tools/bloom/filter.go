package bloom

import (
	"CCdb/errors"
	"CCdb/tools/selfsync"
	"hash/fnv"
	"sync"
)

// BloomFilter 结构定义
type BloomFilter struct {
	size      uint
	hashFuncs uint
	bitset    []bool
	mutex     sync.Locker
}

// Contains 检查元素是否可能存在于布隆过滤器
func (bf *BloomFilter) Contains(key []byte) (bool, error) {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()

	for i := uint(0); i < bf.hashFuncs; i++ {
		value, err := bf.hash(key, i)
		if err != nil {
			return false, err
		}
		index := value % bf.size
		if !bf.bitset[index] {
			return false, nil
		}
	}
	return true, nil
}

// Add 添加一个元素到布隆过滤器
func (bf *BloomFilter) Add(key []byte) error {
	bf.mutex.Lock()
	defer bf.mutex.Unlock()
	for i := uint(0); i < bf.hashFuncs; i++ {
		value, err := bf.hash(key, i)
		if err != nil {
			return err
		}
		index := value % bf.size
		bf.bitset[index] = true
	}
	return nil
}

// NewBloomFilter 创建一个新的布隆过滤器
func NewBloomFilter(size uint, hashFuncs uint) *BloomFilter {
	return &BloomFilter{size: size, hashFuncs: hashFuncs, bitset: make([]bool, size), mutex: selfsync.NewSpinLock()}
}

// 使用fnv把hash算一下吧
func (bf *BloomFilter) hash(key []byte, seed uint) (uint, error) {
	hash := fnv.New64a()
	_, err := hash.Write(key)
	if err != nil {
		return 0, errors.ErrBloomHashFailed
	}
	_, err = hash.Write([]byte{byte(seed)})
	if err != nil {
		return 0, errors.ErrBloomHashFailed
	}
	return uint(hash.Sum64()), nil
}
