package zipper

import (
	"context"

	"storj.io/uplink"
)

const (
	packKey = "github.com/jtolio/zipper:pack"
	packVal = "yes"
)

type Pack struct {
}

// TODO
// var _ fs.FS = (*Pack)(nil)

// TODO
// var _ fs.ReadDirFS = (*Pack)(nil)

func OpenPack(ctx context.Context, p *uplink.Project, bucket, key string) (*Pack, error) {
	panic("TODO")
}

func (p *Pack) IsPackagePack() bool {
	return p.PackInfo().Custom[packKey] == packVal
}

func (p *Pack) PackInfo() *uplink.Object {
	panic("TODO")
}

// TODO
func (p *Pack) List() []string {
	panic("TODO")
}

type FileInfo struct {
	*FileHeader
	Size int64
}

func (p *Pack) FileInfo(ctx context.Context, name string) (*FileInfo, error) {
	panic("TODO")
}

func (fi *FileInfo) Open(ctx context.Context) (*File, error) {
	return &File{}, nil
}

type File struct {
	*FileHeader
	Size int64
}

func (p *Pack) Open(ctx context.Context, name string) (*File, error) {
	return &File{}, nil
}

func (f *File) Read(p []byte) (n int, err error) {
	panic("TODO")
}

func (f *File) Close() error {
	panic("TODO")
}
