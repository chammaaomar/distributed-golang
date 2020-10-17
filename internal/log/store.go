package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var enc = binary.BigEndian

// length of record size in bytes
const lengthWidth = 8

// store is a concurrency-safe wrapper around a file.
// It holds a buffer to reduce the number of writes to disk and
// group together many small writes before flushing to disk, for
// performance improvements.
type store struct {
	File *os.File
	size uint64
	buf  *bufio.Writer
	mu   sync.Mutex
}

func newStore(f *os.File) (*store, error) {
	fileInfo, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := fileInfo.Size()
	return &store{
		buf:  bufio.NewWriter(f),
		size: uint64(size),
		File: f,
	}, nil
}

// Append appends the byte slice to the buffer, preceeded by its length
// as an uint64, without flushing to disk. It returns the number of bytes
// written, the pos and an error
func (s *store) Append(p []byte) (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos := s.size
	// length is an integer, which is usually 64 bits, so we write
	// this in its 8-byte representation
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	written, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	written += lengthWidth
	s.size += uint64(written)
	return uint64(written), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush buffer before reading
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	lengthBytes := make([]byte, lengthWidth)
	if _, err := s.File.ReadAt(lengthBytes, int64(pos)); err != nil {
		return nil, err
	}

	length := enc.Uint64(lengthBytes)

	record := make([]byte, length)
	if _, err := s.File.ReadAt(record, int64(pos+lengthWidth)); err != nil {
		return nil, err
	}

	return record, nil
}

// ReadAt persists buffered data to disk before reading. It reads
// length(p) bytes starting at pos. It implements the io.ReaderAt
// interface
func (s *store) ReadAt(p []byte, pos int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, pos)
}

// Close persists buffered data to disk before closing
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.File.Close()
}
