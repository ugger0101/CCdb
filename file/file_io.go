package file

import "os"

// FileIO 标准系统文件IO
type FileIO struct {
	fd *os.File // 系统文件描述符
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// 文件管理器，返回文件io
func NewFileIOManager(filename string) (*FileIO, error) {
	// DataFilePerm 代表 -rw-r--r-- 文件所有者对该文件有读写权限，用户组和其他人只有读权限，没有执行权限
	fd, err := os.OpenFile(
		filename,
		os.O_CREATE|os.O_RDWR|os.O_APPEND, // 存在就创建文件，赋予读写权限，新数据以追加的方式写入文件
		DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}
