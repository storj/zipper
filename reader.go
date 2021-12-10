package zipper

import (
	"context"
	"io"
	"io/fs"
	"log"
	"strconv"
	"strings"

	"github.com/jtolio/zipper/zipread"
	"github.com/zeebo/errs/v2"
	"storj.io/uplink"
)

const (
	directoryOffsetKey = "github.com/jtolio/zipper:diroffset"
	minTailSearchSize  = 65 * 1024 // the zip reader will guess up to this much
)

type Pack struct {
	info *uplink.Object
	zr   *zipread.Reader
}

func OpenPack(ctx context.Context, proj *uplink.Project, bucket, key string) (*Pack, error) {
	// We're going to store Packs as just plain ZIP files. ZIP files are nice because
	// every file is compressed individually, which means we can decompress single files
	// relatively efficiently, and they are a very widely supported file type, so users
	// can interact with them with their own tools. ZIPs are almost perfect for
	// packs. With a modified zip reading library, we can make it fairly efficient
	// with few round trips.
	//
	// Okay but it's not all roses and here's a thing that stinks. We don't know how
	// long the central directory record is. We have a few options:
	//  * Go read the end of the zip to find out. This sucks because this is not only
	//    a round trip to the Satellite, but also requires storage node traffic just
	//    to find out how long the central directory is, which is then followed by
	//		 subsequent round trips to actually go load it. Yuck.
	//	 * Guess. This is also not great because the central directory can vary in size
	//    wildly. A 32MB zip full of small files can easly take a couple megabytes
	//    for a central directory. If we guess too small of an amount, we don't save
	//    much over the first option, but if we guess a large amount, then we pay a
	//    penalty for every small zip.
	//  * Get metadata about the file first. This isn't bad, but still requires an
	//    extra round trip to the Satellite before we can even request the central
	//    directory. On the positive side, we can store the actual header offset
	//		 in the user metadata, so after one small round trip to the Satellite
	//    (without talking to storage nodes), we can start downloading the central
	//		 directory right after. For ZIPs that don't have this metadata, we could
	//    fall back to one of the prior two options.
	//	 * We could store the central directory in a separate file. This would mean
	//    we wouldn't need more than the download round trip and would be very
	// 	 efficient on the download side, especially if we were lucky enough to
	//    have the central directory inlined. There are two downsides to this:
	//    (1) the whole point of making packs is to reduce small file proliferation,
	//    and (2) this makes our ZIP files mostly (but not completely!)
	//    incompatible with existing tools, and certainly is surprising from a
	//    user perspective. We might as well use our own file format if we go this
	//    route, which would allow us to fix some things like how the central
	// 	 directory entries don't know the exact offset of the compressed file
	//    content. Also, we can't store the central directory itself in the object
	// 	 metadata because it might be too large.
	//  * The best case would be to extend the Satellite to allow us to make a
	//		 conditional range request, where we tell the Satellite to give us
	//    a range based on custom metadata we gave it when we stored the zip.
	//		 This requires some significant protocol, logic, and API changes.
	// Of all of these options, it seems like the worst case is to stat the object,
	// get the custom metadata that says what the central directory size is, then
	// use that to prefetch the tail of the zip. So we're going to do that, but
	// this should absolutely be re-evaluated if:
	//  * We change the file format we're using for packs.
	//  * We extend the Satellite API to allow for custom metadata-based dynamic
	//    range query logic, maybe we embed Google's Common Expression Language
	//    or Starlark or Dhall or something.

	info, err := proj.StatObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}

	var prefetchAmount int64
	if offset, err := getOffset(info); err == nil {
		prefetchAmount = info.System.ContentLength - offset
	}
	if prefetchAmount < minTailSearchSize {
		prefetchAmount = minTailSearchSize
	}

	source, err := zipread.PrefetchTail(ctx, &objectSource{
		proj:   proj,
		bucket: bucket,
		key:    key,
	}, prefetchAmount)
	if err != nil {
		return nil, err
	}

	zr, err := zipread.Open(source)
	if err != nil {
		return nil, err
	}

	return &Pack{
		info: info,
		zr:   zr,
	}, nil
}

func getOffset(info *uplink.Object) (int64, error) {
	return strconv.ParseInt(info.Custom[directoryOffsetKey], 16, 64)
}

func (p *Pack) IsPackagePack() bool {
	_, err := getOffset(p.info)
	return err == nil
}

func (p *Pack) PackInfo() *uplink.Object {
	return p.info
}

// TODO
func (p *Pack) List() []string {
	rv := make([]string, 0, len(p.zr.File))
	for _, f := range p.zr.File {
		if !strings.HasSuffix(f.Name, "/") {
			rv = append(rv, f.Name)
		}
	}
	return rv
}

type FileInfo struct {
	FileHeader
	Size int64

	file *zipread.File
}

func (p *Pack) FileInfo(ctx context.Context, name string) (*FileInfo, error) {
	file, err := p.zr.OpenLookup(name)
	if err != nil {
		return nil, err
	}
	return &FileInfo{
		FileHeader: FileHeader{
			Comment:      file.Comment,
			Modified:     file.Modified,
			Uncompressed: file.Method == zipread.Store,
		},
		Size: int64(file.UncompressedSize64),
		file: file,
	}, nil
}

func (fi *FileInfo) Open(ctx context.Context) (*File, error) {
	rc, err := fi.file.Open()
	if err != nil {
		return nil, err
	}
	return &File{
		FileInfo:   fi,
		ReadCloser: rc,
	}, nil
}

// OpenNative returns the optimal file content based on how it is stored within the zip archive.
// If the compression method is "store" - no compression - the file is trivially returned.
// If the compression method is "deflate" and the client supports gzip, the contents is returned
// without being decompressed, but wrapped as a gzip format.  Other methods return an error.
func (fi *FileInfo) OpenNative(ctx context.Context, allowGzip bool) (*File, bool, error) {
	var err error
	var rc io.ReadCloser
	isDeflate := fi.file.Method == zipread.Deflate
	if allowGzip && isDeflate {
		rc, err = fi.file.OpenAsGzip()
	} else if isDeflate || fi.file.Method == zipread.Store {
		rc, err = fi.file.Open()
	} else {
		return nil, false, zipread.ErrAlgorithm
	}
	if err != nil {
		return nil, false, err
	}
	return &File{FileInfo: fi, ReadCloser: rc}, isDeflate, nil
}

type File struct {
	*FileInfo

	io.ReadCloser
}

func (p *Pack) Open(ctx context.Context, name string) (*File, error) {
	fi, err := p.FileInfo(ctx, name)
	if err != nil {
		return nil, err
	}
	return fi.Open(ctx)
}

func (p *Pack) AsFS(ctx context.Context) fs.FS {
	return p.zr
}

type objectSource struct {
	proj        *uplink.Project
	bucket, key string
}

func (o *objectSource) Range(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	log.Printf("getting range: %d %d", offset, length)
	if offset < 0 || length < 0 {
		return nil, errs.Errorf("negative value")
	}
	dl, err := o.proj.DownloadObject(ctx, o.bucket, o.key, &uplink.DownloadOptions{
		Offset: offset,
		Length: length,
	})
	if err != nil {
		return nil, err
	}
	return dl, nil
}

func (o *objectSource) RangeFromEnd(ctx context.Context, length int64) (rc io.ReadCloser, size int64, err error) {
	log.Printf("getting range from end: %d", length)
	if length < 0 {
		return nil, 0, errs.Errorf("negative value")
	}
	dl, err := o.proj.DownloadObject(ctx, o.bucket, o.key, &uplink.DownloadOptions{
		Offset: -length,
		Length: -1,
	})
	if err != nil {
		return nil, 0, err
	}
	log.Printf("got range from end with overall length %d", dl.Info().System.ContentLength)
	return dl, dl.Info().System.ContentLength, nil
}
