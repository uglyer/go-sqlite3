package main

import (
	"io"
	"os"
)

type demoVFS struct{}

type demoVFSFile struct {
	*os.File
	name string
	f    *os.File
	lock int
}

func (*demoVFS) Open(name string, flags int) (interface{}, error) {
	file, err := os.OpenFile(name, flags, 0600)
	if err != nil {
		return nil, err
	}

	return &demoVFSFile{file, name, file, 0}, nil
}

func xor(b []byte) {
	for i := range b {
		b[i] = b[i] ^ 0xa5
	}
}

func (f *demoVFSFile) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = f.f.ReadAt(p, off)
	if err == io.EOF {
		err = nil
	}
	xor(p)
	return
}

func (f *demoVFSFile) WriteAt(p []byte, off int64) (n int, err error) {
	xor(p)
	n, err = f.f.WriteAt(p, off)
	return
}
