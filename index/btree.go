package index

import (
	"CCdb/data"
	"CCdb/tools/selfsync"
	"github.com/google/btree"
	"sync"
)

type BTree struct {
	tree *btree.BTree // 基于谷歌开源的B树
	lock sync.Locker  // 确保对B树的并发访问线程安全
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.Delete(it)
	if oldItem == nil {
		return false
	}
	return true
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	bt.tree.ReplaceOrInsert(it)
	return true
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: selfsync.NewSpinLock(),
	}
}
