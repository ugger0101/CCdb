package errors

import "errors"

// 暂时的错误
var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrKeyNotFound            = errors.New("key not found in database")
	ErrDataFileNotFound       = errors.New("data file is not found")
	ErrDataDirectoryCorrupted = errors.New("the database directory maybe corrupted")
	ErrInvalidCRC             = errors.New("invalid crc value, log record maybe corrupted")
	ErrInvalidFileSize        = errors.New("database file size must be greater than 0")
	ErrEmptyDirPath           = errors.New("database dir path is empty")
	ErrBloomHashFailed        = errors.New("bloom filter hash error")
	ErrHoldDatabaseFile       = errors.New("Database is used by other ")
)
