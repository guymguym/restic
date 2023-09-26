package location

import (
	"context"
	"net/http"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/limiter"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/restic"
)

type Registry struct {
	factories map[string]Factory
}

func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[string]Factory),
	}
}

func (r *Registry) Register(factory Factory) {
	if r.factories[factory.Scheme()] != nil {
		panic("duplicate backend")
	}
	r.factories[factory.Scheme()] = factory
}

func (r *Registry) Lookup(scheme string) Factory {
	return r.factories[scheme]
}

type Factory interface {
	Scheme() string
	ParseConfig(s string) (interface{}, error)
	StripPassword(s string) string
	Create(ctx context.Context, cfg interface{}, rt http.RoundTripper, lim limiter.Limiter) (restic.Backend, error)
	Open(ctx context.Context, cfg interface{}, rt http.RoundTripper, lim limiter.Limiter) (restic.Backend, error)
	OpenFilesystem(ctx context.Context, cfg interface{}, rt http.RoundTripper, lim limiter.Limiter) (fs.FS, error)
}

type genericBackendFactory[C any, T restic.Backend] struct {
	scheme           string
	parseConfigFn    func(s string) (*C, error)
	stripPasswordFn  func(s string) string
	createFn         func(ctx context.Context, cfg C, rt http.RoundTripper, lim limiter.Limiter) (T, error)
	openFn           func(ctx context.Context, cfg C, rt http.RoundTripper, lim limiter.Limiter) (T, error)
	openFilesystemFn func(ctx context.Context, cfg C, rt http.RoundTripper, lim limiter.Limiter) (fs.FS, error)
}

func (f *genericBackendFactory[C, T]) Scheme() string {
	return f.scheme
}

func (f *genericBackendFactory[C, T]) ParseConfig(s string) (interface{}, error) {
	return f.parseConfigFn(s)
}
func (f *genericBackendFactory[C, T]) StripPassword(s string) string {
	if f.stripPasswordFn != nil {
		return f.stripPasswordFn(s)
	}
	return s
}
func (f *genericBackendFactory[C, T]) Create(ctx context.Context, cfg interface{}, rt http.RoundTripper, lim limiter.Limiter) (backend.Backend, error) {
	return f.createFn(ctx, *cfg.(*C), rt, lim)
}
func (f *genericBackendFactory[C, T]) Open(ctx context.Context, cfg interface{}, rt http.RoundTripper, lim limiter.Limiter) (backend.Backend, error) {
	return f.openFn(ctx, *cfg.(*C), rt, lim)
}
func (f *genericBackendFactory[C, T]) OpenFilesystem(ctx context.Context, cfg interface{}, rt http.RoundTripper, lim limiter.Limiter) (fs.FS, error) {
	return f.openFilesystemFn(ctx, *cfg.(*C), rt, lim)
}

func NewHTTPBackendFactory[C any, T backend.Backend](
	scheme string,
	parseConfigFn func(s string) (*C, error),
	stripPasswordFn func(s string) string,
	createFn func(ctx context.Context, cfg C, rt http.RoundTripper) (T, error),
	openFn func(ctx context.Context, cfg C, rt http.RoundTripper) (T, error),
	openFilesystemFn func(ctx context.Context, cfg C, rt http.RoundTripper) (fs.FS, error),
) Factory {

	return &genericBackendFactory[C, T]{
		scheme:          scheme,
		parseConfigFn:   parseConfigFn,
		stripPasswordFn: stripPasswordFn,
		createFn: func(ctx context.Context, cfg C, rt http.RoundTripper, _ limiter.Limiter) (T, error) {
			return createFn(ctx, cfg, rt)
		},
		openFn: func(ctx context.Context, cfg C, rt http.RoundTripper, _ limiter.Limiter) (T, error) {
			return openFn(ctx, cfg, rt)
		},
		openFilesystemFn: func(ctx context.Context, cfg C, rt http.RoundTripper, _ limiter.Limiter) (fs.FS, error) {
			if openFilesystemFn == nil {
				return nil, errors.Fatalf("unsupported filesystem type %q", scheme)
			}
			return openFilesystemFn(ctx, cfg, rt)
		},
	}
}

func NewLimitedBackendFactory[C any, T backend.Backend](
	scheme string,
	parseConfigFn func(s string) (*C, error),
	stripPasswordFn func(s string) string,
	createFn func(ctx context.Context, cfg C, lim limiter.Limiter) (T, error),
	openFn func(ctx context.Context, cfg C, lim limiter.Limiter) (T, error),
	openFilesystemFn func(ctx context.Context, cfg C, lim limiter.Limiter) (fs.FS, error),
) Factory {

	return &genericBackendFactory[C, T]{
		scheme:          scheme,
		parseConfigFn:   parseConfigFn,
		stripPasswordFn: stripPasswordFn,
		createFn: func(ctx context.Context, cfg C, _ http.RoundTripper, lim limiter.Limiter) (T, error) {
			return createFn(ctx, cfg, lim)
		},
		openFn: func(ctx context.Context, cfg C, _ http.RoundTripper, lim limiter.Limiter) (T, error) {
			return openFn(ctx, cfg, lim)
		},
		openFilesystemFn: func(ctx context.Context, cfg C, _ http.RoundTripper, lim limiter.Limiter) (fs.FS, error) {
			if openFilesystemFn == nil {
				return nil, errors.Fatalf("unsupported filesystem type %q", scheme)
			}
			return openFilesystemFn(ctx, cfg, lim)
		},
	}
}
