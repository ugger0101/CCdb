package data

import (
	"CCdb/errors"
	"CCdb/file"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

// 默认的文件后缀名字
const DataFileNameSuffix = ".data"

type DataFile struct {
	FileID    uint32         // 文件id
	WriteOff  int64          // 文件写到了哪个位置
	IOManager file.IOManager // io读写管理
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	// 文件名字
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)) + DataFileNameSuffix
	// 初始化 IOManager 管理器接口
	ioManager, err := file.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	// 默认从0位置开始写
	return &DataFile{
		FileID:    fileId,
		WriteOff:  0,
		IOManager: ioManager,
	}, nil
}

// 写入文件中，并且偏移量需要向下偏移
func (df *DataFile) Write(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

// 文件持久化
func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

// 读取N个字节
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	return
}

// ReadLogRecord 根据offset 从数据文件中读取LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}
	// 如果读取的最大 header 长度已经超过了文件的长度，则只需要读取到文件的末尾
	var headerBytes = int64(maxLogRecordHeaderSize)
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}
	// 读取Header 信息
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	header, headerSize := decodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	//取出对应的key和value 长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	LogRecord := &LogRecord{
		Key:   nil,
		Value: nil,
		Type:  header.recordType,
	}

	//开始读取用户实际存储的key/value 数据
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		LogRecord.Key = kvBuf[:keySize]
		LogRecord.Value = kvBuf[keySize:]
	}
	// 检验数据有效性
	crc := getLogRecordCRC(LogRecord, headerBuf[crc32.Size:headerSize])
	// 对数据的CRC进行校验
	if crc != header.crc {
		return nil, 0, errors.ErrInvalidCRC
	}
	return LogRecord, recordSize, nil
}
