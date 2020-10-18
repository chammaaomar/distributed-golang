package log

import (
	"fmt"
	"io"
	"os"
	"path"
)

// A segment is an abstraction that manages both a index file and a store file.
// baseOffset and newOffset are both absolute offsets. They are incremented
// over the course of segements in the log. newOffset is the offset at which
// a new entry should be added in the index file.
type segment struct {
	store                 *store
	index                 *index
	baseOffset, newOffset uint64
	config                Config
}

func newSegment(dir string, baseOffset uint64, config Config) (*segment, error) {
	// create the store file; store
	file, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.%s", baseOffset, "store")),
		os.O_CREATE|os.O_APPEND|os.O_RDWR,
		0644,
	)
	if err != nil {
		return nil, err
	}
	store, err := newStore(file)
	if err != nil {
		return nil, err
	}
	// create the index file; index. Can re-assign file ptr since
	// ptrs are passed by value and thus copied in funcs
	file, err = os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.%s", baseOffset, "index")),
		os.O_CREATE|os.O_RDWR,
		0644,
	)
	if err != nil {
		return nil, err
	}
	index, err := newIndex(file, config)
	if err != nil {
		return nil, err
	}
	// determine the relative offset
	relativeOffset, _, err := index.Read(-1)
	if err != nil && err != io.EOF {
		return nil, err
	}
	var newOffset uint64
	if err == io.EOF {
		newOffset = baseOffset
	} else {
		// err is nil
		newOffset = baseOffset + uint64(relativeOffset) + 1
	}
	return &segment{
		index:      index,
		store:      store,
		baseOffset: baseOffset,
		newOffset:  newOffset,
	}, nil
}
