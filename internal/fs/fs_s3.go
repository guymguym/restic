package fs

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
)

// s3FS implements a `FS`
type s3FS struct {
	client *minio.Client
	bucket string
}

// s3File implements `File`
type s3File struct {
	fs     *s3FS
	bucket string
	key    string
	ctx    context.Context
	cancel context.CancelFunc
	object *minio.Object
	list   <-chan minio.ObjectInfo
}

// s3FileInfo implements `fs.FileInfo`
type s3FileInfo struct {
	bucket  string
	key     string
	size    int64
	modTime time.Time
	isDir   bool
}

// statically ensure that S3 implements FS.
var _ FS = &s3FS{}
var _ File = &s3File{}
var _ fs.FileInfo = s3FileInfo{}

func (fs *s3FS) Separator() string               { return "/" }
func (fs *s3FS) VolumeName(path string) string   { return "" }
func (fs *s3FS) Join(elem ...string) string      { return strings.Join(elem, "/") }
func (fs *s3FS) IsAbs(path string) bool          { return strings.HasPrefix(path, "/") }
func (fs *s3FS) Abs(path string) (string, error) { return "/" + strings.TrimPrefix(path, "/"), nil }
func (fs *s3FS) Clean(p string) string           { return filepath.Clean(p) }
func (fs *s3FS) Dir(path string) string          { return filepath.Dir(path) }
func (fs *s3FS) Base(path string) string         { return filepath.Base(path) }

func (fs *s3FS) Open(name string) (File, error)         { return fs.OpenFile(name, 0, 0) }
func (fs *s3FS) Lstat(name string) (os.FileInfo, error) { return fs.Stat(name) }

func (f *s3File) Fd() uintptr  { return 0 }
func (f *s3File) Name() string { return obj_file_name(f.bucket, f.key) }

// func (f s3FileInfo) Sys() any           { return nil }
func (f s3FileInfo) Sys() any           { return &syscall.Stat_t{} }
func (f s3FileInfo) Name() string       { return obj_file_name(f.bucket, f.key) }
func (f s3FileInfo) Size() int64        { return f.size }
func (f s3FileInfo) ModTime() time.Time { return f.modTime }
func (f s3FileInfo) IsDir() bool        { return f.isDir }
func (f s3FileInfo) Mode() fs.FileMode  { return obj_file_mode(f.isDir) }

func NewS3Filesystem(client *minio.Client, bucket string) (*s3FS, error) {
	return &s3FS{client: client, bucket: bucket}, nil
}

func (fs *s3FS) OpenFile(path string, flags int, mode os.FileMode) (File, error) {

	bucket := fs.bucket
	key := strings.TrimPrefix(filepath.Clean(path), "/")
	ctx, cancel := context.WithCancel(context.Background())

	debug.Log("path=%q flags=%x mode=%q bucket=%q key=%q\n", path, flags, mode.String(), bucket, key)
	return &s3File{
		fs:     fs,
		bucket: bucket,
		key:    key,
		ctx:    ctx,
		cancel: cancel,
		object: nil,
		list:   nil,
	}, nil
}

func (fs *s3FS) Stat(path string) (os.FileInfo, error) {
	f, err := fs.OpenFile(path, 0, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Stat()
}

func (f *s3File) Stat() (os.FileInfo, error) {
	debug.Log("bucket=%q key=%q\n", f.bucket, f.key)

	isDir := strings.HasSuffix(f.key, "/")
	size := int64(0)
	modTime := time.Now()

	if !isDir {
		stat, err := f.fs.client.StatObject(f.ctx, f.bucket, f.key, minio.StatObjectOptions{})
		if err == nil {
			size = stat.Size
			modTime = stat.LastModified
		} else {
			err = nil
			prefix := strings.TrimRight(f.key, "/")
			if prefix != "" {
				prefix = prefix + "/"
			}
			ctx, cancel := context.WithCancelCause(f.ctx)
			done := errors.Errorf("done")
			for it := range f.fs.client.ListObjects(ctx, f.bucket, minio.ListObjectsOptions{
				Prefix:  prefix,
				MaxKeys: 2,
			}) {
				if it.Err != nil {
					debug.Log("CHECK DIR ERROR bucket=%q key=%q - %v\n", f.bucket, f.key, it.Err)
					if &err != &it.Err && &err != &done {
						err = it.Err
						cancel(it.Err)
					}
				}
				if it.Key != prefix {
					debug.Log("CHECK DIR OK bucket=%q key=%q it.Key=%q\n", f.bucket, f.key, it.Key)
					isDir = true
					// cancel(done)
				}
			}
			if err != nil && &err != &done {
				return nil, err
			}
		}
	}

	if isDir {
		debug.Log("IS DIR bucket=%q key=%q\n", f.bucket, f.key)
	} else {
		debug.Log("NOT DIR bucket=%q key=%q\n", f.bucket, f.key)
	}

	return s3FileInfo{
		bucket:  f.bucket,
		key:     f.key,
		size:    size,
		modTime: modTime,
		isDir:   isDir,
	}, nil
}

func (f *s3File) Close() error {
	debug.Log("bucket=%q key=%q object=%p list=%p\n", f.bucket, f.key, f.object, f.list)

	f.cancel()

	if f.list != nil {
		for range f.list {
			// drain the list channel
		}
		f.list = nil
	}

	if f.object != nil {
		return f.object.Close()
	}

	return nil
}

func (f *s3File) Read(b []byte) (n int, err error) {
	if f.object == nil {
		_, err := f.Seek(0, io.SeekStart)
		if err != nil {
			return 0, err
		}
	}

	debug.Log("bucket=%q key=%q len=%d\n", f.bucket, f.key, len(b))
	return f.object.Read(b)
}

func (f *s3File) Seek(offset int64, whence int) (int64, error) {
	debug.Log("bucket=%q key=%q object=%p offset=%d whence=%d\n", f.bucket, f.key, f.object, offset, whence)

	if f.object != nil {
		return f.object.Seek(offset, whence)
	}

	options := minio.GetObjectOptions{}
	if offset != 0 {
		if whence == io.SeekEnd {
			err := options.SetRange(0, -offset)
			if err != nil {
				return 0, err
			}
		} else {
			err := options.SetRange(offset, 0)
			if err != nil {
				return 0, err
			}
		}
	}

	object, err := f.fs.client.GetObject(f.ctx, f.bucket, f.key, options)
	if err != nil {
		return 0, err
	}

	f.object = object
	return offset, nil
}

func (f *s3File) Readdir(n int) ([]fs.FileInfo, error) {
	debug.Log("bucket=%q key=%q n=%d list=%v\n", f.bucket, f.key, n, f.list)

	if n <= 0 {
		n = 1000
	}

	files := make([]fs.FileInfo, 0, n)

	prefix := strings.TrimRight(f.key, "/")
	if prefix != "" {
		prefix = prefix + "/"
	}

	if f.list == nil {
		f.list = f.fs.client.ListObjects(f.ctx, f.bucket, minio.ListObjectsOptions{
			Prefix: prefix,
		})
	}

	for it := range f.list {

		if it.Err != nil {
			debug.Log("ERROR bucket=%q key=%q n=%d files=%v error=%v\n", f.bucket, f.key, n, files, it.Err)
			return nil, it.Err
		}

		if it.Key == prefix || it.Key+"/" == prefix {
			debug.Log("SKIP PREFIX bucket=%q key=%q n=%d files=%v it=%+v\n", f.bucket, f.key, n, files, it)
			continue
		}

		isDir := strings.HasSuffix(it.Key, "/")

		files = append(files, s3FileInfo{
			bucket:  f.bucket,
			key:     it.Key,
			size:    it.Size,
			modTime: it.LastModified,
			isDir:   isDir,
		})

		if len(files) >= n {
			debug.Log("BREAK LEN bucket=%q key=%q n=%d len(files)=%d\n", f.bucket, f.key, n, len(files))
			break
		}
	}

	debug.Log("OK bucket=%q key=%q n=%d files=%v\n", f.bucket, f.key, n, files)

	return files, nil
}

func (f *s3File) Readdirnames(n int) ([]string, error) {

	items, err := f.Readdir(n)
	if err != nil {
		return nil, err
	}

	names := make([]string, len(items))
	for i, info := range items {
		names[i] = info.Name()
	}

	return names, nil
}

func obj_file_mode(isDir bool) fs.FileMode {
	if isDir {
		return fs.ModePerm | fs.ModeDir
	} else {
		return fs.ModePerm
	}
}

func obj_file_name(bucket, key string) string {
	if key == "" {
		return bucket
	}
	trimkey := strings.TrimRight(key, "/")
	p := strings.LastIndex(trimkey, "/")
	if p < 0 {
		return trimkey
	}
	return trimkey[p+1:]
}
