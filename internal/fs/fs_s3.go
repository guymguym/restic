package fs

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/restic/restic/internal/errors"
)

// FS3 implements a `FS`
type FS3 struct {
	client *minio.Client
}

// s3File implements `File`
type s3File struct {
	fs3    *FS3
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
var _ FS = &FS3{}
var _ File = &s3File{}
var _ fs.FileInfo = s3FileInfo{}

func (fs3 *FS3) Separator() string               { return "/" }
func (fs3 *FS3) VolumeName(path string) string   { return "" }
func (fs3 *FS3) Join(elem ...string) string      { return strings.Join(elem, "/") }
func (fs3 *FS3) IsAbs(path string) bool          { return strings.HasPrefix(path, "/") }
func (fs3 *FS3) Abs(path string) (string, error) { return "/" + strings.TrimPrefix(path, "/"), nil }
func (fs3 *FS3) Clean(p string) string           { return filepath.Clean(p) }
func (fs3 *FS3) Dir(path string) string          { return filepath.Dir(path) }
func (fs3 *FS3) Base(path string) string         { return filepath.Base(path) }

func (fs3 *FS3) Open(name string) (File, error)         { return fs3.OpenFile(name, 0, 0) }
func (fs3 *FS3) Lstat(name string) (os.FileInfo, error) { return fs3.Stat(name) }

func (f *s3File) Fd() uintptr  { return 0 }
func (f *s3File) Name() string { return obj_file_name(f.bucket, f.key) }

// func (f s3FileInfo) Sys() any           { return nil }
func (f s3FileInfo) Sys() any           { return &syscall.Stat_t{} }
func (f s3FileInfo) Name() string       { return obj_file_name(f.bucket, f.key) }
func (f s3FileInfo) Size() int64        { return f.size }
func (f s3FileInfo) ModTime() time.Time { return f.modTime }
func (f s3FileInfo) IsDir() bool        { return f.isDir }
func (f s3FileInfo) Mode() fs.FileMode  { return obj_file_mode(f.isDir) }

func NewFS3(u *url.URL) (*FS3, error) {

	if u.Path != "" {
		return nil, errors.Fatalf("s3 url should not have a path")
	}

	useHTTP := u.Scheme == "s3+http"
	username := u.User.Username()
	secret, hasSecret := u.User.Password()

	var creds *credentials.Credentials
	if username != "" && secret != "" && hasSecret {
		// if a secret is provided in the url use static credentials
		creds = credentials.NewStaticV4(username, secret, "")
	} else {
		// using the url user as the profile/alias name in the credentials file
		creds = credentials.NewChainCredentials([]credentials.Provider{
			&credentials.EnvAWS{},
			&credentials.EnvMinio{},
			&credentials.FileAWSCredentials{Profile: username},
			&credentials.FileMinioClient{Alias: username},
			&credentials.IAM{
				Client: &http.Client{
					Transport: http.DefaultTransport,
				},
			},
		})
	}

	options := &minio.Options{
		Creds:     creds,
		Secure:    !useHTTP,
		Region:    "global",
		Transport: http.DefaultTransport,
	}

	client, err := minio.New(u.Host, options)
	if err != nil {
		return nil, errors.Wrap(err, "minio.New")
	}

	return &FS3{client}, nil
}

func (fs3 *FS3) OpenFile(path string, flags int, mode os.FileMode) (File, error) {
	fmt.Printf("S3.FS.OpenFile: START path=%q flags=%x mode=%q\n", path, flags, mode.String())

	bucket, key, err := obj_parse_path(path)
	if err != nil {
		fmt.Printf("S3.FS.OpenFile: ERROR path=%q => err=%v\n", path, err)
		return nil, err
	}

	fmt.Printf("S3.FS.OpenFile: OK path=%q => bucket=%q key=%q\n", path, bucket, key)
	ctx, cancel := context.WithCancel(context.Background())
	return &s3File{
		fs3:    fs3,
		bucket: bucket,
		key:    key,
		ctx:    ctx,
		cancel: cancel,
		object: nil,
		list:   nil,
	}, nil
}

func (fs3 *FS3) Stat(path string) (os.FileInfo, error) {
	fmt.Printf("S3.FS.Stat: START path=%q\n", path)

	f, err := fs3.OpenFile(path, 0, 0)
	if err != nil {
		return nil, err
	}

	return f.Stat()
}

func (f *s3File) Stat() (os.FileInfo, error) {
	fmt.Printf("S3.File.Stat: START bucket=%q key=%q\n", f.bucket, f.key)

	isDir := strings.HasSuffix(f.key, "/")
	size := int64(0)
	modTime := time.Now()

	if !isDir {
		stat, err := f.fs3.client.StatObject(f.ctx, f.bucket, f.key, minio.StatObjectOptions{})
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
			for it := range f.fs3.client.ListObjects(ctx, f.bucket, minio.ListObjectsOptions{
				Prefix:  prefix,
				MaxKeys: 2,
			}) {
				if it.Err != nil {
					fmt.Printf("S3.File.Stat: ERROR CHECK DIR bucket=%q key=%q it.Err=%v\n",
						f.bucket, f.key, it.Err)
					if &err != &it.Err && &err != &done {
						err = it.Err
						cancel(it.Err)
					}
				}
				if it.Key != prefix {
					fmt.Printf("S3.File.Stat: CHECK DIR bucket=%q key=%q it.Key=%q\n",
						f.bucket, f.key, it.Key)
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
		fmt.Printf("S3.File.Stat: IS DIR bucket=%q key=%q\n", f.bucket, f.key)
	} else {
		fmt.Printf("S3.File.Stat: NOT DIR bucket=%q key=%q\n", f.bucket, f.key)
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
	fmt.Printf("S3.File.Close: START bucket=%q key=%q object=%p list=%p\n",
		f.bucket, f.key, f.object, f.list)

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
	fmt.Printf("S3.File.Read: START bucket=%q key=%q\n", f.bucket, f.key)

	if f.object == nil {
		_, err := f.Seek(0, io.SeekStart)
		if err != nil {
			return 0, err
		}
	}

	return f.object.Read(b)
}

func (f *s3File) Seek(offset int64, whence int) (int64, error) {
	fmt.Printf("S3.File.Seek: START bucket=%q key=%q object=%p offset=%d whence=%d\n",
		f.bucket, f.key, f.object, offset, whence)

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

	object, err := f.fs3.client.GetObject(f.ctx, f.bucket, f.key, options)
	if err != nil {
		return 0, err
	}

	f.object = object
	return offset, nil
}

func (f *s3File) Readdir(n int) ([]fs.FileInfo, error) {
	fmt.Printf("S3.File.Readdir: START bucket=%q key=%q n=%d list=%v\n",
		f.bucket, f.key, n, f.list)

	if n <= 0 {
		n = 1000
	}

	files := make([]fs.FileInfo, 0, n)

	prefix := strings.TrimRight(f.key, "/")
	if prefix != "" {
		prefix = prefix + "/"
	}

	if f.list == nil {
		f.list = f.fs3.client.ListObjects(f.ctx, f.bucket, minio.ListObjectsOptions{
			Prefix: prefix,
		})
	}

	for it := range f.list {

		if it.Err != nil {
			fmt.Printf("S3.File.Readdir: ERROR bucket=%q key=%q n=%d files=%v error=%v\n",
				f.bucket, f.key, n, files, it.Err)
			return nil, it.Err
		}

		if it.Key == prefix || it.Key+"/" == prefix {
			fmt.Printf("S3.File.Readdir: SKIP PREFIX bucket=%q key=%q n=%d files=%v it=%+v\n",
				f.bucket, f.key, n, files, it)
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
			fmt.Printf("S3.File.Readdir: BREAK LEN bucket=%q key=%q n=%d len(files)=%d\n",
				f.bucket, f.key, n, len(files))
			break
		}
	}

	fmt.Printf("S3.File.Readdir: OK bucket=%q key=%q n=%d files=%v\n",
		f.bucket, f.key, n, files)

	return files, nil
}

func (f *s3File) Readdirnames(n int) ([]string, error) {
	// fmt.Printf("S3.File.Readdirnames: bucket=%q key=%q n=%d\n", f.bucket, f.key, n)

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

func obj_parse_path(path string) (bucket string, key string, err error) {
	// Trim prefix of current working directory
	// if filepath.IsAbs(path) {
	// 	wd, _ := os.Getwd()
	// 	rel, err := filepath.Rel(wd, path)
	// 	if err != nil {
	// 		return "", "", err
	// 	}
	// 	path = rel
	// }
	path = strings.TrimPrefix(filepath.Clean(path), "/")

	// Cut the path to bucket/key
	bucket, key, _ = strings.Cut(path, "/")
	return bucket, key, nil
}
