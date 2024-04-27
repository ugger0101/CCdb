package option

import "CCdb/errors"

func CheckOptions(options Options) error {
	if options.DirPath == "" {
		return errors.ErrEmptyDirPath
	}
	if options.DataFileSize <= 0 {
		return errors.ErrInvalidFileSize
	}
	return nil
}
