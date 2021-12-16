package zipper

import (
	"archive/zip"
	"context"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/zeebo/errs/v2"

	"storj.io/uplink"
)

const (
	contentTypeKey = "content-type"
	contentTypeZip = "application/zip"
)

type PendingPack struct {
	u       *uplink.Upload
	z       *zip.Writer
	counter *countingWriter
	meta    uplink.CustomMetadata
}

func CreatePack(ctx context.Context, proj *uplink.Project, bucket, key string,
	options *uplink.UploadOptions) (*PendingPack, error) {
	u, err := proj.UploadObject(ctx, bucket, key, options)
	if err != nil {
		return nil, err
	}

	counter := &countingWriter{w: u}

	return &PendingPack{
		u:       u,
		z:       zip.NewWriter(counter),
		counter: counter,
	}, nil
}

func (p *PendingPack) SetCustomMetadata(custom uplink.CustomMetadata) {
	if custom != nil {
		custom = custom.Clone()
	}
	p.meta = custom
}

type FileHeader struct {
	Comment      string
	Modified     time.Time
	Uncompressed bool
}

type FileWriter struct {
	io.Writer
}

func (p *PendingPack) Add(ctx context.Context, name string, options *FileHeader) (*FileWriter, error) {
	if strings.HasSuffix(name, "/") {
		return nil, errs.Errorf("adding directories to packs not supported")
	}
	if options == nil {
		options = &FileHeader{}
	}
	header := &zip.FileHeader{
		Name:     name,
		Comment:  options.Comment,
		Modified: options.Modified,
		Method:   zip.Store,
	}
	if options.Uncompressed {
		header.Method = zip.Store
	} else {
		header.Method = zip.Deflate
	}
	w, err := p.z.CreateHeader(header)
	if err != nil {
		return nil, err
	}
	return &FileWriter{
		Writer: w,
	}, nil
}

func (p *PendingPack) Commit(ctx context.Context) error {
	err := p.z.Flush()
	if err != nil {
		err = errs.Combine(err, p.z.Close())
		return errs.Combine(err, p.u.Abort())
	}

	custom := p.meta
	if custom == nil {
		custom = make(uplink.CustomMetadata, 2)
	}

	custom[directoryOffsetKey] = strconv.FormatInt(p.counter.N, 16)
	if _, set := custom[contentTypeKey]; !set {
		custom[contentTypeKey] = contentTypeZip
	}

	err = p.u.SetCustomMetadata(ctx, custom)
	if err != nil {
		err = errs.Combine(err, p.z.Close())
		return errs.Combine(err, p.u.Abort())
	}

	err = p.z.Close()
	if err != nil {
		return errs.Combine(err, p.u.Abort())
	}
	return p.u.Commit()
}

func (p *PendingPack) Abort() error {
	return p.u.Abort()
}

type countingWriter struct {
	N int64
	w io.Writer
}

func (cw *countingWriter) Write(p []byte) (n int, err error) {
	n, err = cw.w.Write(p)
	cw.N += int64(n)
	return n, err
}
