package memory

import (
	"errors"
	"github.com/thanhpd-teko/go-saga/storage"
)

type memStorage struct {
	data []string
}

// NewMemStorage creates log storage base on memory.
// This storage use simple `map[string][]string`, just for TestCase used.
// NOT use this in product.
func NewMemStorage() storage.Storage {
	return &memStorage{
		data: []string{},
	}
}

// AppendLog appends log into queue under given logID.
func (s *memStorage) AppendLog(data string) error {
	s.data = append(s.data, data)
	return nil
}

// Lookup lookups log under given logID.
func (s *memStorage) Lookup() ([]string, error) {
	return s.data, nil
}

// Close uses to close storage and release resources.
func (s *memStorage) Close() error {
	return nil
}

func (s *memStorage) Cleanup() error {
	s.data = []string{}
	return nil
}

func (s *memStorage) LastLog() (string, error) {
	sizeOfLog := len(s.data)
	if sizeOfLog == 0 {
		return "", errors.New("LogData is empty")
	}
	lastLog := s.data[sizeOfLog-1]
	return lastLog, nil
}
