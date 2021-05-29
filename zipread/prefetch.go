package zipread

import (
	"bytes"
	"context"
	"io"

	"github.com/zeebo/errs/v2"
)

type prefetchedTailSource struct {
	s            Source
	size, offset int64
	tail         []byte
}

func PrefetchTail(ctx context.Context, s Source, amount int64) (Source, error) {
	rc, size, err := s.RangeFromEnd(ctx, amount)
	if size < amount {
		amount = size
	}
	tail := make([]byte, amount)
	_, err = io.ReadFull(rc, tail)
	if err != nil {
		return nil, errs.Combine(err, rc.Close())
	}
	return &prefetchedTailSource{
		s:      s,
		size:   size,
		offset: size - amount,
		tail:   tail,
	}, rc.Close()
}

func (s *prefetchedTailSource) Range(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	if length < 0 || offset < 0 {
		return nil, errs.Errorf("negative argument")
	}
	if offset >= s.size {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if offset+length > s.size {
		length = s.size - offset
	}

	if offset >= s.offset {
		return io.NopCloser(bytes.NewReader(s.tail[offset-s.offset:][:length])), nil
	}

	if offset+length <= s.offset {
		return s.s.Range(ctx, offset, length)
	}

	unfetched, err := s.s.Range(ctx, offset, s.offset-offset)
	if err != nil {
		return nil, err
	}

	return struct {
		io.Reader
		io.Closer
	}{
		Reader: io.MultiReader(unfetched, bytes.NewReader(s.tail[:offset+length-s.offset])),
		Closer: unfetched,
	}, nil
}

func (s *prefetchedTailSource) RangeFromEnd(ctx context.Context, length int64) (rc io.ReadCloser, sourceSize int64, err error) {
	rc, err = s.Range(ctx, s.size-length, length)
	return rc, s.size, err
}
