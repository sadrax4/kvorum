package wal

import (
	"bufio"
	"bytes"
)

func newReader(b []byte) *bufio.Reader {
	return bufio.NewReader(bytes.NewReader(b))
}
