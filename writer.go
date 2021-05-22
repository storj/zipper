package zipper

import (
	"archive/zip"
	"context"
	"io"
	"time"

	"github.com/zeebo/errs/v2"

	"storj.io/uplink"
)

type PendingPack struct {
	u *uplink.Upload
	z *zip.Writer
}

func CreatePack(ctx context.Context, p *uplink.Project, bucket, key string,
	options *uplink.UploadOptions) (*PendingPack, error) {
	u, err := p.UploadObject(ctx, bucket, key, options)
	if err != nil {
		return nil, err
	}

	err = u.SetCustomMetadata(ctx, uplink.CustomMetadata{
		"github.com/jtolio/zipper:pack": "yes",
	})
	if err != nil {
		return nil, errs.Combine(err, u.Abort())
	}

	return &PendingPack{
		u: u,
		z: zip.NewWriter(u),
	}, nil
}

func (p *PendingPack) SetCustomMetadata(ctx context.Context, custom uplink.CustomMetadata) error {
	if custom == nil {
		custom = uplink.CustomMetadata{}
	}
	custom = custom.Clone()
	custom[packKey] = packVal
	return p.u.SetCustomMetadata(ctx, custom)
}

type FileHeader struct {
	Comment    string
	Modified   time.Time
	Compressed bool
}

type FileWriter struct {
	io.Writer
}

func (p *PendingPack) Add(name string, options FileHeader) (*FileWriter, error) {
	header := &zip.FileHeader{
		Name:     name,
		Comment:  options.Comment,
		Modified: options.Modified,
		Method:   zip.Store,
	}
	if options.Compressed {
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

func (p *PendingPack) Commit() error {
	err := p.z.Close()
	if err != nil {
		return errs.Combine(err, p.u.Abort())
	}
	return p.u.Commit()
}

func (p *PendingPack) Abort() error {
	return p.u.Abort()
}
