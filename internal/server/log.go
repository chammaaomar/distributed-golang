package server

import (
	"errors"
	"sync"
)

// ErrIllegalOffset is returned when trying trying to index into the log with
// an offset that's greater than the maximum offset in the log
var ErrIllegalOffset = errors.New("Offset is greater than the length of the log")

// Log is a concurrency-safe, append-only collections of Records
type Log struct {
	mu      sync.Mutex
	records []Record
}

// Record is a record in the commit log. It has a byte slice Value, and its Offset
// represents its offset in the commit log
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

// NewLog returns a pointer to a new Log
func NewLog() *Log {
	return &Log{}
}

// Append is appends a Record to the Log in a concurrency-safe manner
func (log *Log) Append(record Record) (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	record.Offset = uint64(len(log.records))
	log.records = append(log.records, record)
	return record.Offset, nil
}

// Read reads from the commit log. It doesn't lock the Log when trying to read,
// but since the log is append-only, it's concurrency-safe
func (log *Log) Read(offset uint64) (Record, error) {
	maxOffset := uint64(len(log.records))
	if offset > maxOffset {
		return Record{}, ErrIllegalOffset
	}
	return log.records[offset], nil
}
