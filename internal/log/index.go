package log

import (
	"io"
	"os"

	"github.com/tysontate/gommap"
)

var (
	offsetWidth uint64 = 4
	posWidth    uint64 = 8
	totalWidth         = offsetWidth + posWidth
)

type index struct {
	size uint64
	File *os.File
	mmap gommap.MMap
}

func newIndex(f *os.File, c Config) (*index, error) {
	fileInfo, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx := &index{File: f}

	idx.size = uint64(fileInfo.Size())
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		f.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

func (idx *index) Close() error {
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := idx.File.Sync(); err != nil {
		return err
	}

	if err := os.Truncate(idx.File.Name(), int64(idx.size)); err != nil {
		return err
	}

	return idx.File.Close()
}

// Read returns the offset and position of the record whose relative offset
// in the index file is relativeIndex. For example, the first record is at
// relativeIndex 0, the second is at relative Index 1, and so on. The returned
// pos is the position in the store file.
func (idx *index) Read(relativeIndex int64) (out uint32, pos uint64, err error) {
	if idx.size == 0 {
		return 0, 0, io.EOF
	}

	if relativeIndex == -1 {
		out = uint32((idx.size / totalWidth) - 1)
	} else {
		out = uint32(relativeIndex)
	}

	pos = uint64(out) * totalWidth
	if pos+totalWidth > idx.size {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(idx.mmap[pos : pos+offsetWidth])
	pos = enc.Uint64(idx.mmap[pos+offsetWidth : pos+totalWidth])

	return out, pos, nil
}

func (idx *index) Write(offset uint32, pos uint64) error {
	if uint64(len(idx.mmap)) < idx.size+totalWidth {
		return io.EOF
	}

	enc.PutUint32(idx.mmap[idx.size:idx.size+offsetWidth], offset)
	enc.PutUint64(idx.mmap[idx.size+offsetWidth:idx.size+totalWidth], pos)
	idx.size += totalWidth
	return nil
}

func (idx *index) Name() string {
	return idx.File.Name()
}
