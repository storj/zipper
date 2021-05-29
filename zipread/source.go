package zipread

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/zeebo/errs/v2"
)

type Source interface {
	Range(ctx context.Context, offset, length int64) (data io.ReadCloser, err error)
	RangeFromEnd(ctx context.Context, length int64) (data io.ReadCloser, sourceLength int64, err error)
}

type FileSource struct {
	name string
}

func SourceFromFile(name string) *FileSource {
	return &FileSource{name: name}
}

func (fs *FileSource) Range(ctx context.Context, offset, length int64) (data io.ReadCloser, err error) {
	if offset < 0 {
		return nil, fmt.Errorf("negative offset")
	}
	fh, err := os.Open(fs.name)
	if err != nil {
		return nil, err
	}
	stat, err := fh.Stat()
	if err != nil {
		return nil, errs.Combine(err, fh.Close())
	}
	if offset >= stat.Size() {
		return io.NopCloser(bytes.NewReader(nil)), fh.Close()
	}
	if offset+length > stat.Size() {
		length = stat.Size() - offset
	}
	_, err = fh.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, errs.Combine(err, fh.Close())
	}
	return struct {
		io.Reader
		io.Closer
	}{
		Reader: io.LimitReader(fh, length),
		Closer: fh,
	}, nil
}

func (fs *FileSource) RangeFromEnd(ctx context.Context, length int64) (data io.ReadCloser, sourceLength int64, err error) {
	if length < 0 {
		return nil, 0, fmt.Errorf("negative length")
	}
	fh, err := os.Open(fs.name)
	if err != nil {
		return nil, 0, err
	}
	stat, err := fh.Stat()
	if err != nil {
		return nil, 0, errs.Combine(err, fh.Close())
	}
	if length > stat.Size() {
		length = stat.Size()
	}
	_, err = fh.Seek(stat.Size()-length, io.SeekStart)
	if err != nil {
		return nil, 0, errs.Combine(err, fh.Close())
	}
	return fh, stat.Size(), nil
}

type ReaderAtSource struct {
	r    io.ReaderAt
	size int64
}

func SourceFromReaderAt(r io.ReaderAt, size int64) *ReaderAtSource {
	if size < 0 {
		panic("negative size")
	}
	return &ReaderAtSource{r: r, size: size}
}

func (ras *ReaderAtSource) Range(ctx context.Context, offset, length int64) (data io.ReadCloser, err error) {
	if offset < 0 {
		return nil, fmt.Errorf("negative offset")
	}
	if offset >= ras.size {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if offset+length > ras.size {
		length = ras.size - offset
	}
	return io.NopCloser(io.NewSectionReader(ras.r, offset, length)), nil
}

func (ras *ReaderAtSource) RangeFromEnd(ctx context.Context, length int64) (data io.ReadCloser, sourceLength int64, err error) {
	if length < 0 {
		return nil, 0, fmt.Errorf("negative length")
	}
	if length > ras.size {
		length = ras.size
	}

	return io.NopCloser(io.NewSectionReader(ras.r, ras.size-length, length)), ras.size, nil
}
