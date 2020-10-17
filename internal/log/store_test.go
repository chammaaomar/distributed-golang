package log

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

var write = []byte("test gopher")
var width = lengthWidth + uint64(len(write))

func TestStoreAppendRead(t *testing.T) {
	file, err := ioutil.TempFile("", "test_append_read")
	require.NoError(t, err)

	s, err := newStore(file)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)

	s, err = newStore(file)
	require.NoError(t, err)
	testRead(t, s)
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
