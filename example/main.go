package main

import (
	"CCdb"
	"CCdb/option"
	"fmt"
)

func main() {
	opts := option.DefaultOptions
	fmt.Println("------------", opts.DirPath)
	opts.DirPath = "D:\\Go projects\\CCdb\\tempdata"

	db, err := CCdb.Open(opts)
	if err != nil {
		fmt.Println("111111")
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		fmt.Println("22222")
		panic(err)
	}

	val, err := db.Get([]byte("name"))
	if err != nil {
		fmt.Println("33333")
		panic(err)
	}

	fmt.Println("val = ", string(val))

	err = db.Put([]byte("name"), []byte("bitcask1"))
	if err != nil {
		fmt.Println("22222")
		panic(err)
	}
	val, err = db.Get([]byte("name"))
	if err != nil {
		fmt.Println("33333")
		panic(err)
	}
	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		fmt.Println("44444")
		panic(err)
	}
}
