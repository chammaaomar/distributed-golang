package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var write = []byte("test gopher")
var width = lengthWidth + uint64(len(write))

func TestStoreAppendRead(t *testing.T) {
	file, err := ioutil.TempFile("", "test_append_read")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	s, err := newStore(file)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(file)
	require.NoError(t, err)
	testRead(t, s)
}

// test the store file persists to disk upon closing
func TestStoreClose(t *testing.T) {
	file, err := ioutil.TempFile("", "test_close")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	s, err := newStore(file)
	require.NoError(t, err)

	_, _, err = s.Append(write)
	require.NoError(t, err)

	fileInfo, err := os.Stat(file.Name())
	require.NoError(t, err)

	beforeSize := fileInfo.Size()

	err = s.Close()
	require.NoError(t, err)

	fileInfo, err = os.Stat(file.Name())
	require.NoError(t, err)

	afterSize := fileInfo.Size()

	require.Equal(t, width, uint64(afterSize-beforeSize))
}

// tests appending several elements
func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := 1; i < 4; i++ {
		written, offset, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, uint64(i)*width, offset+written)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var offset uint64
	for i := 1; i < 4; i++ {
		read, err := s.Read(offset)
		require.NoError(t, err)
		require.Equal(t, write, read)
		offset += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	var offset int64
	for i := 1; i < 4; i++ {
		p := make([]byte, width)
		bytesRead, err := s.ReadAt(p, offset)
		require.NoError(t, err)

		require.Equal(t, width, uint64(bytesRead))

		length := enc.Uint64(p[:lengthWidth])
		require.Equal(t, uint64(len(write)), length)

		read := p[lengthWidth:]
		require.Equal(t, write, read)

		offset += int64(width)
	}
}
