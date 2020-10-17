package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	file, err := ioutil.TempFile("", "test_index")
	require.NoError(t, err)
	defer file.Close()

	config := Config{}

	config.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(file, config)
	require.NoError(t, err)

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	t.Run("If we write entries to the index and read them back, we should get the same thing", func(t *testing.T) {
		for _, expected := range entries {
			err := idx.Write(expected.Off, expected.Pos)
			require.NoError(t, err)

			actualOff, actualPos, err := idx.Read(int64(expected.Off))
			require.NoError(t, err)

			require.Equal(t, expected.Off, actualOff)
			require.Equal(t, expected.Pos, actualPos)
		}
	})

	t.Run("Trying to read beyond the bounds should return an EOF error", func(t *testing.T) {
		_, _, err := idx.Read(int64(len(entries)))
		require.Equal(t, io.EOF, err)
	})

	t.Run("Closing the index and re-opening it should end up in the same state", func(t *testing.T) {
		err := idx.Close()
		require.NoError(t, err)

		file, err := os.OpenFile(file.Name(), os.O_RDWR, 0660)
		require.NoError(t, err)
		defer file.Close()

		idx, err := newIndex(file, config)
		require.NoError(t, err)

		offset, pos, err := idx.Read(-1)

		require.NoError(t, err)
		require.Equal(t, entries[1].Off, offset)
		require.Equal(t, entries[1].Pos, pos)

		offset, pos, err = idx.Read(0)

		require.NoError(t, err)
		require.Equal(t, entries[0].Off, offset)
		require.Equal(t, entries[0].Pos, pos)
	})

	t.Run("Calling Name on Index file should return the name of underlying file", func(t *testing.T) {
		require.Equal(t, file.Name(), idx.Name())
	})
}
