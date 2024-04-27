package test

import (
	"CCdb"
	"CCdb/option"
	"fmt"
	"testing"
)

func TestDb(t *testing.T) {
	fmt.Println("-----test-----")
	db, err := CCdb.Open(option.Options{
		DirPath:      "D:/Go projects/CCdb",
		DataFileSize: 256 * 1024 * 1024,
		SyncWrites:   true,
		IndexType:    1,
	})
	if err != nil {
		t.Errorf("err: %v", err)
	}
	fmt.Println("-----test1-----")
	err = db.Put([]byte("k1"), []byte("v1"))
	if err != nil {
		t.Errorf("err: %v", err)
	}
	fmt.Println("-----test2-----")
	get, err := db.Get([]byte("k1"))
	fmt.Println("----value = ", get)
	if err != nil {
		t.Errorf("err: %v", err)
	}
	fmt.Println("----value = ", get)
}
